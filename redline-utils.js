const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');
var s3urls = require('s3urls');
const moment = require('moment');
const Got = require('got');

const RETRY_COUNT = 3;
const MOUNT_PATH = 'avflow-efs';

module.exports = {

  sleep: async (ms) => {
    return new Promise(resolve => setTimeout(resolve, ms));
  },

  getServiceEnv: async (index, params) => {
    return new Promise((resolve, reject) => {
      let lambda = new AWS.Lambda();
      lambda.invoke({
        InvocationType: 'RequestResponse',
        Payload: JSON.stringify({ index: index, id: params.workflowExcutionId }),
        FunctionName: 'arn:aws:lambda:' + params.region + ':' + params.accountId + ':avflow-stepfunction-functions-' + params.stage + '-getENV'
      }, (err, data) => {
        if (err) reject(err);

        let payload = JSON.parse(data.Payload);
        if (payload.body) {
          let body = JSON.parse(payload.body)
          resolve(body.data);
        } else {
          reject(payload);
        }
      });
    })
  },

  updateWFJobId: async (params, jobId) => {
    //Update JobID to current Workflow History
    let lambda = new AWS.Lambda();
    let payload = {
      jobId: jobId,
      workflowHistoryId: params.workflowHistoryId,
      workflowExcutionId: params.workflowExcutionId
    }
    let execution = lambda.invokeAsync({
      FunctionName: 'arn:aws:lambda:' + params.region + ':' + params.accountId + ':function:avflow-workflow-functions-' + params.stage + '-addJobId',
      InvokeArgs: JSON.stringify(payload)
    });
    await execution.promise();
  },

  formatS3SourceUrl: (sourceUrl) => {
    try {
      sourceUrl.replace(/\+/g, "%20"); // space code: %20
      let s3Object = s3urls.fromUrl(sourceUrl);
      if (s3Object.Key.indexOf('+')) s3Object.Key = s3Object.Key.replace(/\+/g, " ");
      console.log('S3 FILE OBJECT', s3Object);
      return s3Object;
    } catch (e) {
      return null;
    }
  },

  getFileExtention: (fileName) => {
    try {
      let extension = fileName.split('.').pop();
      console.log('FILE EXETENTION', extension);
      return extension;
    } catch (error) {
      throw error;
    }
  },

  checkS3SourceExist: async (S3, s3Object) => {
    try {
      return await new Promise((resolve, reject) => {
        S3.headObject({ Bucket: s3Object.Bucket, Key: s3Object.Key }, (err, data) => {
          if (err) reject(false)
          else resolve(s3Object);
        });
      })
    } catch (e) {
      throw e;
    }
  },

  generateEFSOutputFile: (service, params, index, fileName, extension) => {
    return {
      extension: extension,
      fileName: `${index}_${fileName}`,
      filePath: `${params.workflowExcutionId}/${service.name}`,
      location: `${params.workflowExcutionId}/${service.name}/${index}_${fileName}.${extension}`,
      filePath: `${params.workflowExcutionId}/${service.name}`
    }
  },

  generateEFSOutputFileMulti: (service, params, index, fileName, extension) => {
    return {
      extension: extension,
      fileName: `${index}_${fileName}`,
      filePath: `${params.workflowExcutionId}/${service.name}`
    }
  },

  getEFSLocation: (params, tagInfo, fileName, extension = null, index = null) => {
    try {
      if (!tagInfo) return false;
      if (!extension) extension = module.exports.getFileExtention(fileName);

      let efsFileName = `${tagInfo[0]}_${tagInfo[2]}`;
      if (index != null) efsFileName = `${efsFileName}_${index}`;

      let efsInfos = {
        service: tagInfo[1],
        extension: extension,
        fileName: efsFileName,
        efsLocation: `${tagInfo[1]}/${efsFileName}.${extension}`
      }
      console.log('EFS INFO', efsInfos);
      return efsInfos;
    } catch (e) {
      throw e;
    }
  },

  getTagValue: (tagValue) => {
    try {
      if (tagValue.match(/\[\[(.*?)\]\]/g)) {
        let tagInfo = tagValue.match(/\[\[(.*?)\]\]/g)[0].replace(/^\[\[(.+)\]\]$/, '$1').split('.');
        console.log('TAG INFO', tagInfo);
        return tagInfo;
      }
      return null;
    } catch (error) {
      console.log('TAG ERROR', error);
      return null;
    }
  },

  getFileName: (s3Key) => {
    try {
      if (s3Key.indexOf('/') >= 0) {
        let splits = s3Key.split('/');
        return splits[splits.length - 1];
      }
      return s3Key;
    } catch (err) {
      console.log('Get file name error: ', err);
    }
  },

  isEFSExist: (serviceName, fileName) => {
    try {
      let files = fs.readdirSync(`${process.cwd()}/${serviceName}`);
      for (let file of files) if (file.includes(fileName)) return file;
      return false;
    } catch (error) {
      return false;
    }
  },

  getEFSFile: async (service, params, sourceKey, tagInfo, index = null) => {
    try {
      console.log("START GET EFS FILE", sourceKey, tagInfo);
      let url = service[`${sourceKey}_value`] ? service[`${sourceKey}_value`] : service[sourceKey];
      console.log("URL FILE EFS: ", url);
      let s3Object = module.exports.formatS3SourceUrl(url);
      if (s3Object) {
        s3Object['url'] = url;
        let efsFileInfos = module.exports.getEFSLocation(params, tagInfo, s3Object.Key, null, index);
        if (module.exports.isEFSExist(efsFileInfos.service, efsFileInfos.fileName))
          return { ...s3Object, efsLocation: efsFileInfos.efsLocation, extension: efsFileInfos.extension };

        let downloadedEFS = await module.exports.downloadS3Source(service, params, url, efsFileInfos.efsLocation, tagInfo);
        return { ...s3Object, ...efsFileInfos, efsLocation: downloadedEFS };
      } else if (url) {
        console.log('2')
        //1_0_result.mp3
        if (module.exports.getTagValue(url)) return null;
        let efsFileInfos = module.exports.getEFSLocation(params, tagInfo, url, null, index);
        if (module.exports.isEFSExist(efsFileInfos.service, efsFileInfos.fileName)) {
          return { ...efsFileInfos, url: url, extension: efsFileInfos.extension  };
        } else {
          return { url: url };
        }
      } else {
        throw new Error(`${sourceKey} is not found`);
      }
    } catch (error) {
      console.log('GET EFS FILE ERROR', error);
      throw error;
    }
  },

  downloadS3Source: async (service, params, url, efsLocation, tagInfo) => {
    async function saveToEFS(url, efsFileInfos, filePath) {
      console.log('save file', efsFileInfos, filePath);
      return await new Promise((resolve, reject) => {
        let efsFile = fs.createWriteStream(filePath)
          .on('close', () => { resolve(efsFileInfos) })
          .on('error', (err) => { reject(err); })
          .on('finish', () => {
            console.log('DOWNLOADED', fs.readdirSync(`./${tagInfo[1]}`));
            efsFile.end();
          });

        let downloading = Got.stream(url)
          .on('downloadProgress', (progress) => {
            if (progress.percent == 1) console.log('DOWNLOAD COMPLETED');
          })
          .on('error', (err) => { reject(err) });

        downloading.pipe(efsFile);
      })
    }

    try {
      if (!fs.existsSync(`./${efsLocation.split('/').shift()}`))
        module.exports.makeServiceDir(`./${tagInfo[1]}`);

      let counter = 0;
      let filePath = path.resolve(`/${MOUNT_PATH}/${params.workflowExcutionId}`, `${efsLocation}`);
      try {
        return await saveToEFS(url, efsLocation, filePath);
      } catch (error) {
        counter += 1;
        if (counter > RETRY_COUNT) throw error;
        return await saveToEFS(url, efsLocation, filePath);
      }
    } catch (err) {
      console.log('DOWNLOAD S3 FAIL', err);
      throw new Error('Get source data fail.');
    }
  },
  copyToUserBucket: async (destinationBucket, outputfile) => {
    console.log('Copy file to user bucket');
    const s3UserOptions = {
      accessKeyId: SERVICE.keyId,
      secretAccessKey: SERVICE.secretKey,
      signatureVersion: 'v4'
    }
    const s3User = new AWS.S3(s3UserOptions);
    const s3Object = s3urls.fromUrl(outputfile);
    const inputBucketName = s3Object.Bucket;
    const inputKeyName = s3Object.Key;
    const outPutKey = 'burn-in/' + inputKeyName;
    const copyParams = {
      CopySource: inputBucketName + '/' + inputKeyName,
      Bucket: destinationBucket,
      Key: outPutKey
    }
    console.log('Copy object params: ', copyParams)
    return new Promise((resolve, reject) => {
      s3User.copyObject(copyParams, (err, data) => {
        console.log('Copy object resutl: ', err, data)
        if (err) reject("Generate output file error.");
        var fileUrl = s3User.getSignedUrl('getObject', { Bucket: destinationBucket, Key: outPutKey, Expires: 8600 });
        console.log('Out put file url: ', fileUrl);
        resolve(fileUrl);
      });
    })
  },

  makeServiceDir: (createPath, recursive = true) => {
    fs.mkdirSync(createPath, { recursive: recursive });
    return;
  },

  invokeLambdaResponse: async (functionName, params) => {
    console.log('CHARGE CREDIT', params)
    return await new Promise((resolve, reject) => {
      let lambda = new AWS.Lambda();
      let payload = {
        FunctionName: functionName,
        InvocationType: 'RequestResponse',
        Payload: JSON.stringify(params)
      }
      lambda.invoke(payload, function (err, data) {
        if (err) reject(err);
        if (data) {
          const payload = JSON.parse(data.Payload)
          if (payload.body) {
            const body = JSON.parse(payload.body)
            resolve(body);
          } else resolve(payload)
        }
      });
    })
  },
  validationExtention: async (extention) => {
    console.log('validationExtention', extention)
    if(extention.toLowerCase() === 'braw') {
        return false
    }
    else {
      return true
    }
}

}
