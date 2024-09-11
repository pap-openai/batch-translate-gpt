// index.js

import pLimit from 'p-limit';
import { OpenAI } from 'openai';
import { Readable } from 'stream';
import { createObjectCsvStringifier } from 'csv-writer';
import csvParser from 'csv-parser';
import path from 'path';

// Initialize OpenAI API
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

// Configuration
const MAX_BATCH_SIZE = process.env.MAX_BATCH_SIZE || 10;
const MAX_CONCURRENCY_REQ = process.env.MAX_CONCURRENCY_REQ || 100;
const MODEL_NAME = process.env.MODEL_NAME || 'gpt-4o-mini';

/**
 * Parses CSV data into an array of rows.
 * @param {string} csvData - The CSV data as a string.
 * @returns {Promise<Object[]>} - Array of row objects.
 */
const parseCsvData = async (csvData) => {
  const rows = [];
  return new Promise((resolve, reject) => {
    Readable.from(csvData)
      .pipe(csvParser({}))
      .on('data', (data) => rows.push(data))
      .on('end', () => resolve(rows))
      .on('error', (error) => reject(error));
  });
};

/**
 * Converts an array of rows back to CSV format.
 * @param {Object[]} rows - Array of row objects.
 * @returns {string} - CSV data as a string.
 */
const writeCsvData = (rows) => {
  if (rows.length === 0) return '';
  const header = Object.keys(rows[0]);
  const csvStringifier = createObjectCsvStringifier({
    header: header.map((h) => ({ id: h, title: h })),
  });
  return csvStringifier.getHeaderString() + csvStringifier.stringifyRecords(rows);
};

/**
 * Translates an array of texts from the source language to the target language.
 * @param {Object} params - Parameters for translation.
 * @param {string[]} params.texts - Texts to translate.
 * @param {string} params.sourceLang - Source language.
 * @param {string} params.targetLang - Target language.
 * @param {string} [params.promptTemplate] - Optional prompt template.
 * @returns {Promise<Object[]>} - Array of translation objects.
 */
const translateTextBatch = async ({ texts, sourceLang, targetLang }) => {
  try {
    const prompt = `
You are a professional translator. Translate the following texts from ${sourceLang} to ${targetLang}. Preserve the original meaning and context. Provide the translations in a JSON array format as shown below.

Texts to translate:
${texts.map((text, index) => `${index + 1}. ${text}`).join('\n')}
`;

    const response = await openai.chat.completions.create({
      model: MODEL_NAME,
      messages: [
        {
          role: 'system',
          content:
            'You are a professional translator capable of providing accurate and context-aware translations.',
        },
        {
          role: 'user',
          content: prompt.trim(),
        },
      ],
      // Using structured outputs
      response_format: {
        type: 'json_schema',
        json_schema: {
          name: 'translation_output',
          strict: true,
          schema: {
            type: 'object',
            properties: {
              translations: {
                type: 'array',
                items: {
                  type: 'object',
                  properties: {
                    original_text: { type: 'string' },
                    translated_text: { type: 'string' },
                  },
                  required: ['original_text', 'translated_text'],
                  additionalProperties: false,
                },
              },
            },
            required: ['translations'],
            additionalProperties: false,
          },
        },
      },
    });

    const content = JSON.parse(response.choices[0].message.content.trim()).translations;
    return content;
  } catch (error) {
    console.error('Error in translateTextBatch:', error);
    throw error;
  }
};

/**
 * Prepares and executes translation requests.
 * @param {Object[]} requests - Array of translation requests.
 * @param {string} sourceLang - Source language.
 * @param {string} targetLang - Target language.
 * @returns {Promise<Object[]>} - Array of translations mapped back to requests.
 */
const prepareAndExecuteTranslations = async (requests, sourceLang, targetLang) => {
  try {
    const translationPromises = [];
    const limit = pLimit(MAX_CONCURRENCY_REQ);

    for (let i = 0; i < requests.length; i += MAX_BATCH_SIZE) {
      const batch = requests.slice(i, i + MAX_BATCH_SIZE);
      const texts = batch.map((req) => req.text);

      const batchPromise = limit(async () => {
        const translations = await translateTextBatch({
          texts,
          sourceLang,
          targetLang,
        });
        return { translations, batch };
      });

      translationPromises.push(batchPromise);
    }

    const translationResults = await Promise.all(translationPromises);

    // Map translations back to their requests
    const allTranslations = [];
    translationResults.forEach(({ translations, batch }) => {
      translations.forEach((translation, index) => {
        allTranslations.push({
          original_text: translation.original_text,
          translated_text: translation.translated_text,
        });
      });
    });

    return allTranslations;
  } catch (error) {
    console.error('Error in prepareAndExecuteTranslations:', error);
    throw error;
  }
};

/**
 * Processes a single CSV file, translating missing cells.
 * @param {Object} csv_data - The CSV data to be processed.
 * @param {string} fileName - The name of the CSV file.
 * @returns {Promise<Object>} - A promise that resolves to the translated file data.
 */
const processFile = async (csv_data, fileName) => {
  try {
    // Parse CSV data
    const rows = await parseCsvData(csv_data);

    if (rows.length === 0) {
      throw new Error(`The CSV file ${fileName} is empty.`);
    }

    // Prepare translation requests
    const translationRequests = [];

    rows.forEach((row, rowIndex) => {
      const columns = Object.keys(row);
      columns.forEach((targetLang) => {
        if (!row[targetLang] || row[targetLang].trim() === '') {
          const sourceLang = columns.find(
            (col) => col !== targetLang && row[col] && row[col].trim() !== ''
          );
          if (sourceLang) {
            // Validate source and target languages
            validateLanguageCode(sourceLang);
            validateLanguageCode(targetLang);

            translationRequests.push({
              rowIndex,
              sourceLang,
              targetLang,
              text: row[sourceLang],
            });
          }
        }
      });
    });

    if (translationRequests.length === 0) {
      console.log(`No translations needed for file: ${fileName}`);
    } else {
      // Group requests by language pair
      const requestsByLangPair = {};

      translationRequests.forEach((request) => {
        const { sourceLang, targetLang } = request;
        const key = `${sourceLang}-${targetLang}`;
        if (!requestsByLangPair[key]) {
          requestsByLangPair[key] = [];
        }
        requestsByLangPair[key].push(request);
      });

      // Process translations for each language pair
      for (const [langPair, requests] of Object.entries(requestsByLangPair)) {
        const [sourceLang, targetLang] = langPair.split('-');

        const translations = await prepareAndExecuteTranslations(
          requests,
          sourceLang,
          targetLang
        );
        // Update rows with translations
        requests.forEach(({ rowIndex, targetLang, text }) => {
          const translatedText = translations.find(
            (translation) => translation.original_text === text
          )?.translated_text;

          if (translatedText) {
            rows[rowIndex][targetLang] = translatedText;
          }
        });

        console.log(`Completed translations for ${sourceLang} to ${targetLang} in file: ${fileName}`);
      }
    }

    // Write translated data to CSV format
    const translatedCsvData = writeCsvData(rows);

    // Encode to base64
    const fileContent = Buffer.from(translatedCsvData).toString('base64');

    console.log(`Finished processing file: ${fileName}`);

    return {
      name: `translated_${fileName}`,
      mime_type: 'text/csv',
      content: fileContent,
    };
  } catch (error) {
    console.error(`Error processing file ${fileName}:`, error);
    throw error;
  }
};

/**
 * Translates an entire CSV file into the specified language.
 * @param {Object} csvData - csvData content
 * @param {string} lang - Target language for translation.
 * @returns {Promise<Object>} - Translated file data.
 */
const translateCsvToLanguage = async (csvData, fileName, lang) => {
  try {
    validateLanguageCode(lang);
    console.log(`Starting translation for file: ${fileName} to language: ${lang}`);

    // Parse CSV data
    const rows = csvData.split('\n').map(row => row.split(','));

    if (rows.length === 0) {
      throw new Error(`The CSV file ${fileName} is empty.`);
    }

    console.log(`Parsed CSV data into rows`);

    // Extract unique texts for translation (if a text is repeated in multiple cells, we'll call the API only once)
    const textsSet = new Set();
    rows.forEach((row) => {
      Object.values(row).forEach((cell) => {
        if (cell && cell.trim() !== '') {
          textsSet.add(cell);
        }
      });
    });

    const texts = Array.from(textsSet);

    console.log(`Extracted unique cells for translation: ${texts.length} unique cells`);

    // Prepare translation requests
    const translationRequests = texts.map((text) => ({ text }));

    // Execute translations
    const translations = await prepareAndExecuteTranslations(
      translationRequests,
      'any language', // hardcoded "any language" as input but might be worth incorporating this as a parameter rather than hardcoded
      lang
    );

    // Create a map for quick lookup of translated texts
    const translationMap = new Map();
    translations.forEach(({ original_text, translated_text }) => {
      translationMap.set(original_text, translated_text);
    });

    console.log(`Created translation map`);

    const translatedRows = rows.map(row => {
      return row.map(cell => translationMap.get(cell) || cell);
    });

    console.log(`Reconstructed translated rows`);

    // Write translated data to CSV format
    const translatedCsvData = translatedRows.map(row => row.join(',')).join('\n');

    // Encode to base64
    const fileContent = Buffer.from(translatedCsvData).toString('base64');

    console.log(`Translation complete for file: ${fileName}`);

    return {
      name: `translated_${path.basename(fileName)}`,
      mime_type: 'text/csv',
      content: fileContent,
    };
  } catch (error) {
    console.error(`Error translating CSV file ${fileName}:`, error);
    throw error;
  }
};

/**
 * Validates if the provided language code is supported.
 * @param {string} lang - Language code to validate.
 */
const validateLanguageCode = (lang) => {
  const supportedLanguages = [
    'English', 'Spanish', 'French', 'German', 'Chinese', 'Japanese', 
    'Korean', 'Russian', 'Arabic', 'Portuguese', 'Italian', 'Dutch', 
    'Greek', 'Hebrew', 'Hindi', 'Norwegian', 'Polish', 'Swedish', 
    'Turkish', 'Vietnamese', 'Thai', 'Indonesian', 'Malay', 'Bengali', 
    'Punjabi', 'Tamil', 'Telugu', 'Marathi', 'Gujarati', 'Kannada', 
    'Urdu', 'Persian', 'Ukrainian', 'Romanian', 'Hungarian', 'Czech', 
    'Slovak', 'Bulgarian', 'Croatian', 'Serbian', 'Finnish', 'Danish', 
    'Icelandic', 'Filipino', 'Swahili', 'Zulu', 'Afrikaans', 'Amharic', 
    'Yoruba', 'Hausa', 'Igbo', 'Burmese', 'Khmer', 'Lao', 'Sinhala', 
    'Nepali', 'Pashto', 'Somali', 'Tigrinya', 'Mongolian', 'Kazakh', 
    'Uzbek', 'Tajik', 'Kyrgyz', 'Turkmen', 'Azerbaijani', 'Georgian', 
    'Armenian', 'Albanian', 'Bosnian', 'Macedonian', 'Montenegrin', 
    'Slovenian', 'Latvian', 'Lithuanian', 'Estonian', 'Maltese', 
    'Luxembourgish', 'Welsh', 'Irish', 'Scottish Gaelic', 'Breton', 
    'Basque', 'Galician', 'Catalan', 'Occitan', 'Corsican', 'Sardinian'
  ];
  if (!supportedLanguages.map(language => language.toLowerCase().trim()).includes(lang.toLowerCase().trim())) {
    throw new Error(`Unsupported language code: ${lang}`);
  }
};

/**
 * Handles translation requests by processing files and returning translated files.
 * @param {Object} req - Request object.
 * @param {Object} res - Response object.
 */
const translateHandler = async (req, res) => {
  try {
    const { openaiFileIdRefs, language } = req.body;   

    if (!Array.isArray(openaiFileIdRefs) || openaiFileIdRefs.length === 0) {
      return res.status(400).json({ error: 'Invalid or empty "openaiFileIdRefs" array.' });
    }

    const translatedFiles = await Promise.all(
      openaiFileIdRefs.map(async (fileRef) => {
        const response = await fetch(fileRef.download_link);
        if (!response.ok) {
          throw new Error(`Failed to download file: ${fileRef.name}`);
        }
        const csvData = await response.text();
        // if we have the language parameter we actually translate the CSV itself
        // if we don't, we actually assume that columns are languages and we'll fill in empty cells
        return language ? translateCsvToLanguage(csvData, fileRef.name, language) : processFile(csvData, fileRef.name);
      })
    );
    res.json({ openaiFileResponse: translatedFiles });
  } catch (error) {
    console.error('Unexpected error:', error);
    res.status(500).json({ error: error.message || 'Internal Server Error' });
  }
};

// Export the function for your server setup
export { translateHandler };
