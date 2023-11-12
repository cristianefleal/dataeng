const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

AWS.config.update({ region: 'us-east-1' });
const kinesis = new AWS.Kinesis();
const streamName = 'stream-data';

const directoryPath = '/app/dataeng'; 

function generateShortKey(data) {
  const hash = crypto.createHash('sha256').update(data).digest('hex');
  return hash.substring(0, 8);
}

fs.readdir(directoryPath, (err, files) => {
  if (err) {
    console.error('Erro ao ler o diretório:', err);
    return;
  }

  // Itera sobre cada arquivo no diretório
  files.forEach(file => {
    // Verifica se o arquivo possui a extensão .json
    if (path.extname(file).toLowerCase() === '.json') {
      const filePath = path.join(directoryPath, file);
      const fileName = path.basename(file);

      fs.readFile(filePath, 'utf8', (err, data) => {
        if (err) {
          console.error(`Erro ao ler o arquivo ${filePath}:`, err);
          return;
        }

        try {
          const jsonData = JSON.parse(data);
          const campos = jsonData.campos.map(record => {
            return { ...record };
          });
          campos.forEach(record => {
            const partitionKey = generateShortKey(JSON.stringify(record));
            const params = {
              Data: JSON.stringify(record),
              PartitionKey: partitionKey,
              StreamName: streamName
            };
            kinesis.putRecord(params, (err, data) => {
              if (err) {
                console.error('Erro ao inserir registro no Kinesis:', err);
              } else {
                console.log('Registro inserido no Kinesis:', data);
              }
            });
          });

          console.log('Registros do arquivo', filePath, 'enviados para o Kinesis.');
        } catch (parseErr) {
          console.error(`Erro ao analisar o JSON do arquivo ${filePath}:`, parseErr);
        }
      });
    }
  });
});
