const elasticsearch = require('elasticsearch');
const httpAwsEs = require('http-aws-es');
const moment = require('moment-timezone');
const AWS = require('aws-sdk');

AWS.config.update({ region: 'us-east-1' });

function getEnv(key, defaultValue) {
  const value = process.env[key];
  if (!value) {
    if (defaultValue) return defaultValue;
    throw new Error(`Expected env var ${key} to be set`);
  }
  return value;
}

async function sendAlert(litresConsumed, startDate, alert, testMode) {
  const sns = new AWS.SNS();
  const TopicArn = getEnv('ALERT_TOPIC_ARN');
  // eslint-disable-next-line no-template-curly-in-string
  const reportUrl = getEnv('KIBANA_MONTHLY_REPORT_URL').replace('{startDate}', startDate.toISOString());
  const Message = alert
    ? `ALERTA: Se ha excedido el limite para el mes con ${litresConsumed}L. \nVer:${reportUrl}`
    : `AVISO: Se ha consumido ${litresConsumed}L del mes. \nVer:${reportUrl}`;

  if (testMode) {
    // eslint-disable-next-line no-console
    console.log(Message);
    return Promise.resolve();
  }

  return sns.publish({ Message, TopicArn }).promise();
}

module.exports.run = async (event) => {
  const host = getEnv('ES_ENDPOINT');
  const billingDay = parseInt(getEnv('BILLING_DAY', '26'), 10);
  const maxLitresPerMonth = parseInt(getEnv('MONTHLY_LITRE_LIMIT', '25000'), 10);

  const client = new elasticsearch.Client({
    host,
    connectionClass: httpAwsEs,
    amazonES: {
      region: 'us-east-1',
      credentials: new AWS.EnvironmentCredentials('AWS'),
    },
  });

  const now = moment().tz('America/Santiago');
  const startDate = now.subtract(now.date() > billingDay ? 0 : 1, 'month')
    .date(billingDay).startOf('day');

  const body = await client.search({
    index: 'water-meter-reading',
    body: {
      size: 0,
      query: {
        range: {
          ts: {
            from: startDate.toISOString(),
            to: 'now',
            include_lower: true,
            include_upper: true,
            boost: 1,
          },
        },
      },
      aggregations: {
        litres: {
          sum: {
            field: 'ticks',
            script: {
              source: '_value / 24.0',
              lang: 'painless',
            },
          },
        },
      },
    },
  });

  const litresConsumed = body.aggregations.litres.value.toFixed(1);
  const alarm = litresConsumed > maxLitresPerMonth;
  await sendAlert(litresConsumed, startDate, alarm, event.testMode);

  return {
    statusCode: 200,
    body: JSON.stringify({ litresConsumed, startDate: startDate.toISOString(), alarm }),
  };
};
