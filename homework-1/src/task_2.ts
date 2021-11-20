import fs from 'fs';
import split2 from 'split2';
import { pipeline, PassThrough, Transform } from 'stream';
import csv from 'csvtojson';

const PATH_TO_CSV = './nodejs-hw1-ex1.csv';
const OUTPUT_FILE = './csvOutput.txt';

let header: null | string = null;

const testStream = new PassThrough();

testStream.on('data', (data) => {
  console.log(data.toString());
});

const fakeDataBase = (data: string) => {
  return new Promise<string>((resolve) => {
    setTimeout(() => {
      resolve(data);
    }, 1000);
  });
};

const writeToDBStream = new Transform({
  transform: async (chunk: Buffer, _, callback) => {
    const dataRow = chunk.toString();
    try {
      const response = await fakeDataBase(dataRow);
      console.log('Data was written successfully', response);
      callback(null, dataRow);
    } catch (error) {
      console.log('Error writing to database', error);
      callback(null, dataRow);
    }
  },
});

const transformStream = new Transform({
  transform: async (chunk: Buffer, _, callback) => {
    if (!header) {
      header = chunk.toString();
      callback();
    } else {
      const line = chunk.toString();
      const fromCsvToArray = await csv().fromString(`${header}\n${line}`);
      const stringified = `${JSON.stringify(fromCsvToArray[0])}\n`;
      callback(null, stringified);
    }
  },
});

const convertFromCsv = async (pathToCsv: string, outputPath: string) => {
  try {
    await pipeline(
      fs.createReadStream(pathToCsv),
      split2(),
      transformStream,
      writeToDBStream,
      fs.createWriteStream(outputPath),
      (error) => {
        if (error) {
          console.log(error.message);
        }
      }
    );
  } catch (error) {
    if (error instanceof Error) {
      console.error(error.message);
    }
  }
};

convertFromCsv(PATH_TO_CSV, OUTPUT_FILE);
