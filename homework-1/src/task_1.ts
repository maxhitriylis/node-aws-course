const stdinStream = process.stdin;

const reverseString = (string: string) => string.split("").reverse().join("");

stdinStream.on("data", (input) => {
  const stringFromBuffer = input.toString();
  const reversedString = reverseString(stringFromBuffer);
  console.log(reversedString);
});
