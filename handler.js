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

async function sendAlert(litresConsumedForDay, litresConsumedForMonth, startDate, testMode) {
  const maxLitresPerMonth = parseInt(getEnv('MONTHLY_LITRE_LIMIT', '25000'), 10);
  const monthlyTriggerValue = maxLitresPerMonth * 0.85;

  const sns = new AWS.SNS();
  const TopicArn = getEnv('ALERT_TOPIC_ARN');
  // eslint-disable-next-line no-template-curly-in-string
  const reportUrl = getEnv('KIBANA_MONTHLY_REPORT_URL').replace('{startDate}', startDate.toISOString());
  const Message = litresConsumedForMonth > monthlyTriggerValue
    ? `ALERTA: Excedidó 85% del limite del mes con ${litresConsumedForMonth}L ${litresConsumedForDay}L en el ultimo 24 horas. \nVer:${reportUrl}`
    : `AVISO: Se ha consumido ${litresConsumedForDay}L durante el ultimo 24 horas y ${litresConsumedForMonth}L del mes. \nVer:${reportUrl}`;

  if (testMode) {
    // eslint-disable-next-line no-console
    console.log(Message);
    return Promise.resolve();
  }

  return sns.publish({ Message, TopicArn }).promise();
}

function getTz() {
  return getEnv('TIMEZONE', 'America/Santiago');
}

async function getLitresConsumed(client, from) {
  const query = {
    index: 'water-meter-reading',
    body: {
      size: 0,
      query: {
        range: {
          ts: {
            from,
            to: 'now',
            include_lower: true,
            include_upper: true,
            boost: 1,
            time_zone: getTz(),
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
  };

  const result = await client.search(query);
  return result.aggregations.litres.value.toFixed(1);
}

module.exports.run = async (event) => {
  const host = getEnv('ES_ENDPOINT');
  const billingDay = parseInt(getEnv('BILLING_DAY', '26'), 10);

  const client = new elasticsearch.Client({
    host,
    connectionClass: httpAwsEs,
    amazonES: {
      region: 'us-east-1',
      credentials: new AWS.EnvironmentCredentials('AWS'),
    },
  });

  const now = moment().tz(getTz());
  const startDate = now.subtract(now.date() > billingDay ? 0 : 1, 'month')
    .date(billingDay).startOf('day');

  const litresConsumedForMonth = await getLitresConsumed(client, startDate);
  const litresConsumedForDay = await getLitresConsumed(client,'now-24h');
  await sendAlert(litresConsumedForDay, litresConsumedForMonth, startDate, event.testMode);

  return {
    statusCode: 200,
    body: JSON.stringify({
      litresConsumedForDay,
      litresConsumedForMonth,
      startDate: startDate.toISOString(),
    }),
  };
};
