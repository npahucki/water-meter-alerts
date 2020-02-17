/* eslint-disable no-console */

const handler = require('../handler');

handler.run({ testMode: true })
  .then((r) => {
    console.log(r.body);
    process.exit(0);
  })
  .catch((e) => {
    console.error(e.stack);
    process.exit(1);
  });
