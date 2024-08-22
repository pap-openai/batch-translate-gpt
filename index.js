// index.js

import pLimit from 'p-limit';
import { OpenAI } from 'openai';
import csvParser from 'csv-parser';
import { createObjectCsvWriter } from 'csv-writer';
import fs from 'fs';
import os from 'os';
import path from 'path';
import { pipeline } from 'stream/promises';


// Initialize OpenAI API
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// Configuration
const MAX_BATCH_SIZE = 10; // Adjust based on API limits and performance considerations
const MAX_CONCURRENCY_REQ = 100;
const MODEL_NAME = 'gpt-4o-mini'; 

// Function to translate texts in batches using structured output
const translateTextBatch = async ({ texts, sourceLang, targetLang }) => {
  try {
    const prompt = `
You are a professional translator. Translate the following texts from ${sourceLang} to ${targetLang}. Preserve the original meaning and context. Provide the translations in a JSON array format as shown below.

Texts to translate:
${texts.map((text, index) => `${index + 1}. ${text}`).join('\n')}
`;
    console.log(prompt)
    const response = await openai.chat.completions.create({
      model: MODEL_NAME,
      messages: [
        {
          role: 'system',
          content: 'You are a professional translator capable of providing accurate and context-aware translations.',
        },
        {
          role: 'user',
          content: prompt.trim(),
        },
      ],
      // using Structured Ouputs: https://platform.openai.com/docs/guides/structured-outputs/introduction
      response_format: {
        "type": "json_schema",
        "json_schema": {
            "name": "translation_output",
            "strict": true,
            schema: {
                "type": "object",
                "properties": {
                    "translations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "original_text": {"type": "string"},
                                "translated_text": {"type": "string"}
                            },
                            "required": ["original_text", "translated_text"],
                            "additionalProperties": false
                        }
                    },
                },
                "required": ["translations"],
                "additionalProperties": false
    
            }
        }
      }
    });

    const content = JSON.parse(response.choices[0].message.content.trim()).translations;
    return content;
  } catch (error) {
    console.error('Error in translateTextBatch:', error);
    throw error;
  }
};

// Function to process a single file
const processFile = async (fileRef) => {
  const { name, download_link, mime_type } = fileRef;

  if (mime_type !== 'text/csv') {
    throw new Error(`Unsupported file type: ${mime_type}. Only CSV files are supported.`);
  }

  console.log(`Processing file: ${name}`);

  const response = await fetch(download_link);
  if (!response.ok) {
    throw new Error(`Failed to download the file: ${name}`);
  }

  // Create temporary files for input and output
  const timestamp = Date.now();
  const tempInputFilePath = path.join(os.tmpdir(), `input_${timestamp}_${name}`);
  const tempOutputFilePath = path.join(os.tmpdir(), `translated_${timestamp}_${name}`);

  try {
    // Write the fetched data to the temp input file
    await pipeline(response.body, fs.createWriteStream(tempInputFilePath));

    const rows = [];
    await new Promise((resolve, reject) => {
      fs.createReadStream(tempInputFilePath)
        .pipe(csvParser())
        .on('data', (data) => rows.push(data))
        .on('end', resolve)
        .on('error', reject);
    });

    if (rows.length === 0) {
      throw new Error(`The CSV file ${name} is empty.`);
    }

    // Prepare translation requests
    const translationRequests = {};

    rows.forEach((row, rowIndex) => {
      const columns = Object.keys(row);
      columns.forEach((targetLang) => {
        if (!row[targetLang] || row[targetLang].trim() === '') {
          const sourceLang = columns.find(
            (col) => col !== targetLang && row[col] && row[col].trim() !== ''
          );
          if (sourceLang) {
            const key = `${sourceLang}-${targetLang}`;
            if (!translationRequests[key]) {
              translationRequests[key] = [];
            }
            translationRequests[key].push({
              rowIndex,
              sourceLang,
              targetLang,
              text: row[sourceLang],
            });
          }
        }
      });
    });

    // Process translations for each language pair
    // Global concurrency limit. Could adopt that based on your OpenAI Rate Limit
    // https://platform.openai.com/docs/guides/rate-limits/usage-tiers
    const globalLimit = pLimit(MAX_CONCURRENCY_REQ);

    const allLanguagePromises = [];
    
    for (const [langPair, requests] of Object.entries(translationRequests)) {
      const [sourceLang, targetLang] = langPair.split('-');
    
      const batchPromises = [];
    
      for (let i = 0; i < requests.length; i += MAX_BATCH_SIZE) {
        const batch = requests.slice(i, i + MAX_BATCH_SIZE);
        const texts = batch.map((req) => req.text);
    
        const batchPromise = globalLimit(() =>
          translateTextBatch({
            texts,
            sourceLang,
            targetLang,
          }).then((translations) => {
            translations.forEach((translation, index) => {
              const { rowIndex } = batch[index];
              rows[rowIndex][targetLang] = translation.translated_text;
            });
            console.log(
              `Translated batch ${i / MAX_BATCH_SIZE + 1} for ${sourceLang} to ${targetLang}`
            );
          })
        );
    
        batchPromises.push(batchPromise);
      }
    
      allLanguagePromises.push(...batchPromises);
    }
    
    await Promise.all(allLanguagePromises);
    
    // Write translated data to a new CSV file
    const headers = Object.keys(rows[0]).map((key) => ({
      id: key,
      title: key,
    }));

    const csvWriter = createObjectCsvWriter({
      path: tempOutputFilePath,
      header: headers,
    });

    await csvWriter.writeRecords(rows);

    // Read the translated CSV file and encode it to base64
    const fileContent = fs.readFileSync(tempOutputFilePath, {
      encoding: 'base64',
    });

    console.log(`Finished processing file: ${name}`);

    return {
      name: `translated_${name}`,
      mime_type: 'text/csv',
      content: fileContent,
    };
  } catch (error) {
    console.error(`Error processing file ${name}:`, error);
    throw error;
  } finally {
    // Clean up temporary files
    fs.unlink(tempInputFilePath, (err) => {
      if (err) console.error(`Error deleting temp input file: ${err}`);
    });
    fs.unlink(tempOutputFilePath, (err) => {
      if (err) console.error(`Error deleting temp output file: ${err}`);
    });
  }
};

// Main function to handle translation requests
const translateHandler = async (req, res) => {
  try {
    const { openaiFileIdRefs } = req.body;

    if (!Array.isArray(openaiFileIdRefs) || openaiFileIdRefs.length === 0) {
      return res
        .status(400)
        .json({ error: 'Invalid or empty "openaiFileIdRefs" array.' });
    }

    const translatedFiles = await Promise.all(
      openaiFileIdRefs.map((fileRef) => processFile(fileRef))
    );

    res.json({
      openaiFileResponse: translatedFiles,
    });
  } catch (error) {
    console.error('Unexpected error:', error);
    res.status(500).json({ error: error.message || 'Internal Server Error' });
  }
};

// Export the function for your server setup
export { translateHandler };

