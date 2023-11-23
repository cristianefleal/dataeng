const AWS = require('aws-sdk');

AWS.config.update({ region: 'us-east-1' });
const kinesis = new AWS.Kinesis();
const streamName = 'data-stream';

function generateRandomLogRecord() {
          const timestamp = new Date().toISOString();
          const severity = getRandomSeverity();
          const message = generateRandomMessage();
          const requestID = generateRandomRequestID();
          const headers = generateRandomHeaders();
          const response = getRandomResponseCode();
          const record = {
                      timestamp,
                      severity,
                      message,
                      requestID,
                      headers,
                      response,
                    };
          return JSON.stringify(record);
}

function generateRandomRequestID() {
  return 'request-' + Math.floor(Math.random() * 1000);
}

function generateRandomHeaders() {
  const userAgents = [
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0',
    'Opera/9.80 (Macintosh; Intel Mac OS X; U; en) Presto/2.2.15 Version/10.00',
    'Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0)',
  ];

  const randomUserAgent = userAgents[Math.floor(Math.random() * userAgents.length)];

  const headers = {
    'User-Agent': randomUserAgent,
    'Accept-Language': 'pt-BR',
  };

  return headers;

}

function getRandomResponseCode() {
    const responseCodes = [200, 201, 400, 401, 404, 500];
    return responseCodes[Math.floor(Math.random() * responseCodes.length)];
}

function getRandomSeverity() {
          const severities = ['info', 'warning', 'error'];
          return severities[Math.floor(Math.random() * severities.length)];
}

function generateRandomMessage() {
          const messages = ['This is a sample log message', 'Something happened', 'An error occurred'];
          return messages[Math.floor(Math.random() * messages.length)];
}


const putRecord = async () => {
    const data = generateRandomLogRecord();

    console.log('Registro a ser inserido:', data, '\n');

    const record = JSON.parse(data); 
    const params = {
                Data: data,
                PartitionKey: record.severity, 
                StreamName: streamName,
              };

    try {
                const response = await kinesis.putRecord(params).promise();
              } catch (err) {
                          console.error('Erro ao inserir registro:', err);
                        }
    setTimeout(putRecord, 100);
};

putRecord();

