const { nanoid } = require('nanoid');
const gelfserver = require('graygelf/server');
const http = require('http');
const {BigQuery} = require('@google-cloud/bigquery');
const elasticsearch = require('elasticsearch');

const PORT = process.env.PORT || 12201;
const BIGQUERY_DATASET = process.env.BIGQUERY_DATASET;
const BIGQUERY_TABLE = process.env.BIGQUERY_TABLE;
const bigqueryClient = new BigQuery();

async function insert(data) {
  // Creates a client
  const {
    id,
    container,
    tag,
    message,
    timestamp,
    raw_data,
    level
  } = data;

  const rows = [
    {
      id,
      container,
      tag,
      message,
      timestamp,
      level,
      raw_data
    }
  ];

  // Insert data into table
  await bigqueryClient
    .dataset(BIGQUERY_DATASET)
    .table(BIGQUERY_TABLE)
    .insert(rows);
}

const server = gelfserver();

const parseJSON = (message) => {
  try {
    const data = JSON.parse(message);
    return data;
  } catch (err) {
    return false;
  }
}

server.on('message', async gelf => {
  const { host, timestamp, short_message, _container_name, level } = gelf;
  const id = nanoid();

  const data = parseJSON(short_message);
  const message = data?.message ? data.message : short_message;
  try {
    await insert({
      id,
      container: _container_name,
      tag: host,
      message,
      timestamp,
      level: level,
      raw_data: JSON.stringify(gelf)
    });
  } catch(err) {
    console.log(err)
  }
});

server.listen(PORT, '0.0.0.0');
console.info('listening...')



const requestListener = function (req, res) {
  res.writeHead(200);
  res.end('ok');
}
const httpServer = http.createServer(requestListener);
httpServer.listen(80);



