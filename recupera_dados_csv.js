const AWS = require('aws-sdk');
const readline = require('readline');
const stream = require('stream');

AWS.config.update({ region: 'us-east-1' }); 
const s3 = new AWS.S3();

async function consolidateCSVFiles(bucketName, folderPath, destinationKey) {
  const files = await listFilesInFolder(bucketName, folderPath);
  
  if (files.length === 0) {
    console.log('Nenhum arquivo encontrado para consolidar.');
    return;
  }

  const consolidatedData = await consolidateData(bucketName, files);

 
  await saveConsolidatedData(bucketName, destinationKey, consolidatedData);

  console.log('Consolidação concluída e arquivo salvo:', destinationKey);
}

async function listFilesInFolder(bucketName, folderPath) {
  const params = {
    Bucket: bucketName,
    Prefix: folderPath,
  };

  const response = await s3.listObjectsV2(params).promise();
  return response.Contents.map(obj => obj.Key);
}

async function consolidateData(bucketName, files) {
  const consolidatedData = [];

  for (const fileKey of files) {
    const fileData = await downloadFile(bucketName, fileKey);
    const fileLines = fileData.split('\n');

    if (consolidatedData.length === 0) {
      consolidatedData.push(fileLines[0]);
    }

    
    for (let i = 1; i < fileLines.length; i++) {
      const line = fileLines[i].trim();
      if (line.length > 0) {
        consolidatedData.push(line);
      }
    }
  }

  return consolidatedData.join('\n');
}

async function downloadFile(bucketName, fileKey) {
  const params = {
    Bucket: bucketName,
    Key: fileKey,
  };

  const response = await s3.getObject(params).promise();
  return response.Body.toString('utf-8');
}

async function saveConsolidatedData(bucketName, destinationKey, data) {
  const params = {
    Bucket: bucketName,
    Key: destinationKey,
    Body: data,
  };

  await s3.putObject(params).promise();
}

const bucketName = 'projetoimpacta-grupo0001-destination-gold';
const folderPath = 'gold/';
const destinationKey = 'resultado_final.csv';

consolidateCSVFiles(bucketName, folderPath, destinationKey);
