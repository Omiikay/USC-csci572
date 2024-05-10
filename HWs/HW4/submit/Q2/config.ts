import { Config } from "./src/config";

export const defaultConfig: Config = {
  url: "https://www.tensorflow.org/api_docs/python/tf",
  match: "https://www.tensorflow.org/api_docs/python/tf/**",
  maxPagesToCrawl: 20,
  outputFileName: "output.json",
  maxTokens: 200000,
};
