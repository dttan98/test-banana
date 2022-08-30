const fs = require('fs');
const AWS = require('aws-sdk');
const moment = require('moment');
const RP = require('request-promise');
var StepFunction = new AWS.StepFunctions();

const Utils = require('./redline-utils');
let START_TIME, RESET_TIME, INTERVAL, SERVICE;
const INDEX = process.env.INDEX;
const PARAMS = JSON.parse(process.env.PARAMS);
const MY_TASK_TOKEN_ENV = process.env.MY_TASK_TOKEN_ENV;

const TIME_OUT = 3 * 60 * 60 * 1000;
const INTERVAL_TIME = 60 * 1000;
const GET_STATUS_INTERVAL = 1000 * 30;
const EXEUTION_DURATION = 'execution duration';
const TOKEN = "YXZmbG93LWZmbXBlZy1zZXJ2aWNlLWRldi0wNy8xMC8yMDIw";
const REDLINE_ENDPOINT = "https://afihz53983.execute-api.us-east-1.amazonaws.com/production/job"

const ACTIONS = {
    TRANSCODE: "transcode"
}

const SERVICE_TYPES = {
    TRANSCODE: "TRANSCODE"
}

const PAYMENT_STATUS = {
    SUCCESS: 'SUCCESS',
    IN_PROGRESS: 'IN_PROGRESS'
}

async function reportErr(err) {
    console.log('TASK FAILED', fs.readdirSync('./'));
    try{
        if(err.message === 'out of credit') {
            throw err
        }
        let [duration, chargeAmount] = await handleChargeCredit();

        if (SERVICE.credit) chargeAmount += SERVICE.credit;
    
        let params = {
            taskToken: MY_TASK_TOKEN_ENV,
            cause: JSON.stringify({
                index: parseInt(INDEX) + 1, params: PARAMS, output: {
                    error: err.message ? err.message : err,
                    credit: chargeAmount,
                    [EXEUTION_DURATION]: duration,
                }
            })
        }
        console.log('TASK FAILED', duration);
        await StepFunction.sendTaskFailure(params).promise();
        process.exit(1);
    }
    catch (err) {
        let params = {
            taskToken: MY_TASK_TOKEN_ENV,
            cause: JSON.stringify({
                index: parseInt(INDEX) + 1, params: PARAMS, output: {
                    error: err.message ? err.message : err,
                }
            })
        }
        console.log('TASK FAILED', params);
        await StepFunction.sendTaskFailure(params).promise();
        process.exit(1);
    }
}

async function reportBack(result, warnings) {
    console.log('TASK SUCCED', fs.readdirSync('./'));
    let [duration, chargeAmount] = await handleChargeCredit(PAYMENT_STATUS.SUCCESS);

    if (SERVICE.credit) chargeAmount += SERVICE.credit;

    let params = {
        taskToken: MY_TASK_TOKEN_ENV,
        output: JSON.stringify({
            index: parseInt(INDEX) + 1, params: PARAMS,
            output: { ...result, [EXEUTION_DURATION]: duration, credit: chargeAmount, warnings: warnings }
        })
    }
    await StepFunction.sendTaskSuccess(params).promise();
    process.exit(1);
}

var handleChargeCredit = async (status = PAYMENT_STATUS.IN_PROGRESS) => {
    try{
        clearInterval(INTERVAL);
        const endTime = moment();
        let chargeDuration = Math.round(moment.duration(endTime.diff(RESET_TIME)).as('milliseconds'));
        let executionDuration = Math.round(moment.duration(endTime.diff(START_TIME)).as('milliseconds') / 1000);
        let chargeAmount = await intervalCharge(START_TIME.valueOf().toString(), SERVICE.name, PARAMS, status, chargeDuration);
        return [executionDuration, chargeAmount];
    }
    catch(err) {
        throw err
    }
}

async function intervalCharge(sessionId, serviceName, params, status, duration = INTERVAL_TIME) {
    RESET_TIME = moment();
    let payload = {
        status: status,
        userId: params.userId,
        planId: params.userPlan,
        workflowId: params.workflowId,
        serviceName: serviceName,
        workspaceId: params.workspaceId,
        executionId: params.workflowExcutionId,
        action: 'CHARGE_CREDIT',
        chargeSession: sessionId,
        duration: duration
    }

    let functionName = 'arn:aws:lambda:' + params.region + ':' + params.accountId + ':avflow-payment-service-' + params.stage + '-chargeCredit';
    let result = await Utils.invokeLambdaResponse(functionName, payload);
    if (result.data) {
        if (result.data.credit) {
            return result.data.credit
        }
        else {
            return 0
        }
    }
    else {
        throw Error('out of credit')
    }
}

async function redlineTranscode() {
    try {
        var jobID = await getTranscodeJob();
        if (jobID) {
            let arrayFileUrl = []
            var output = await processing(jobID);
            console.log('data out', output)
            let fileUrl, oggAudioUrl, mp3AudioUrl, properties, mp4VideoUrl, frameFilePathUrl, transcodeUrl
            if (output) {
                switch (SERVICE.action) {
                    case ACTIONS.TRANSCODE:
                        transcodeUrl = output.transcodeUrl.split('/').pop();
                        break;
                    default:
                        transcodeUrl = output.transcodeUrl;
                }
            }
            if (arrayFileUrl.length > 0) {
                return await reportBack({ array: arrayFileUrl });
            }
            else {
                return await reportBack({ transcodeUrl });
            }
        } else {
            await reportErr(new Error("Create transocde job fail."));
        }
    } catch (err) {
        console.log('FFmpeg transcode error: ', err);
        await reportErr(err);
    }
}

const getTranscodeJob = async () => {
    let jobID = null;
    try {
        if (PARAMS.jobId) {
            jobID = PARAMS.jobId;
            delete PARAMS['jobId'];
            console.log('Existing jobId: ', jobID);
            return jobID;
        }
        jobID = await createJob();
        console.log('Transcode job ID: ', jobID);
        await Utils.updateWFJobId(PARAMS, jobID);
        return jobID;
    } catch (err) {
        throw err;
    }
}

const createJob = async () => {
    console.log("Create transcode job");
    try {
        let params = {
            serviceName: 'redline',
            workflowId: PARAMS.workflowId,
            workflowExecutionId: PARAMS.workflowExcutionId,
        }

        let valid = true

        switch (SERVICE.action) {
            case ACTIONS.TRANSCODE:
                let transcodeVideo = await Utils.getEFSFile(SERVICE, PARAMS, 'videoSource', Utils.getTagValue(SERVICE.videoSource));
                console.log('transcodeVideo: ', transcodeVideo);
                if(transcodeVideo.extension.toLowerCase() !== "r3d") {
                    await reportErr(new Error("Unsupported input format."));
                }
                let outputFormat = SERVICE.outputFormat_value ? SERVICE.outputFormat_value : SERVICE.outputFormat
                if(outputFormat && !['ProRes 422 HQ', 'ProRes 422', 'ProRes 422 LT', 'ProRes 422 Proxy', 'ProRes 4444', 'ProRes 4444 XQ'].includes(outputFormat)) {
                    await reportErr(new Error("Output Format invalid. Possible values(ProRes 422 HQ, ProRes 422, ProRes 422 LT, ProRes 422 Proxy, ProRes 4444, ProRes 4444 XQ)"));
                }
                let colorSpace = SERVICE.colorSpace_value ? SERVICE.colorSpace_value : SERVICE.colorSpace
                let gammaCurve = SERVICE.gammaCurve_value ? SERVICE.gammaCurve_value : SERVICE.gammaCurve
                let resizeX = SERVICE.resizeX_value ? SERVICE.resizeX_value : SERVICE.resizeX
                let resizeY = SERVICE.resizeY_value ? SERVICE.resizeY_value : SERVICE.resizeY
                let  colorSpaceKey = [{
                    title: "REDspace",
                    value: "2"
                },
                {
                    title: "CameraRGB",
                    value: "0"
                },
                {
                    title: "BT.709",
                    value: "1"
                },
                {
                    title: "REDcolor",
                    value: "14"
                },
                {
                    title: "sRGB",
                    value: "15"
                },
                {
                    title: "Adobe1998",
                    value: "5"
                },
                {
                    title: "REDcolor2",
                    value: "18"
                },
                {
                    title: "REDcolor3",
                    value: "19"
                },
                {
                    title: "DRAGONcolor",
                    value: "20"
                },
                {
                    title: "XYZ",
                    value: "21"
                },
                {
                    title: "REDcolor4",
                    value: "22"
                },
                {
                    title: "DRAGONcolor2",
                    value: "23"
                },
                {
                    title: "BT.2020",
                    value: "24"
                },
                {
                    title: "REDWideGamutRGB",
                    value: "25"
                },
                {
                    title: "DCI-P3",
                    value: "26"
                }
                    ,
                {
                    title: "DCI-P3 D65",
                    value: "27"
                }
                ]
                let gammaCurveKey = [
                    {
                        title: "REDspace",
                        value: "14"
                    },
                    {
                        title: "linear",
                        value: "-1"
                    },
                    {
                        title: "BT.709",
                        value: "1"
                    },
                    {
                        title: "sRGB",
                        value: "2"
                    },
                    {
                        title: "REDlog",
                        value: "3"
                    },
                    {
                        title: "PDLog985",
                        value: "4"
                    },
                    {
                        title: "PDLog685",
                        value: "5"
                    },
                    {
                        title: "PDLogCustom",
                        value: "6"
                    },
                    {
                        title: "REDgamma",
                        value: "15"
                    },
                    {
                        title: "REDLogFilm",
                        value: "27"
                    },
                    {
                        title: "REDgamma2",
                        value: "28"
                    },
                    {
                        title: "REDgamma3",
                        value: "29"
                    },
                    {
                        title: "REDgamma4",
                        value: "30"
                    },
                    {
                        title: "ST 2084",
                        value: "31"
                    },
                    {
                        title: "BT.1886",
                        value: "32"
                    },
                    {
                        title: "Log3G12",
                        value: "33"
                    },
                    {
                        title: "Log3G10",
                        value: "34"
                    },
                    {
                        title: "Hybrid Log-Gamma",
                        value: "35"
                    },
                    {
                        title: "Gamma 2.2",
                        value: "36"
                    },
                    {
                        title: "Gamma 2.6",
                        value: "37"
                    }
            
                ]
                colorSpaceKey.map((item) => {
                    if (item.title == colorSpace) {
                        colorSpace = item.value
                    }
                })
                gammaCurveKey.map((item) => {
                    if (item.title == gammaCurve) {
                        gammaCurve = item.value
                    }
                })
                if(outputFormat === 'ProRes 422 HQ') {
                    outputFormat = '0'
                }
                else if(outputFormat === 'ProRes 422') {
                    outputFormat = '1'
                }
                else if(outputFormat === 'ProRes 422 LT') {
                    outputFormat = '2'
                }
                else if(outputFormat === 'ProRes 422 Proxy') {
                    outputFormat = '3'
                }
                else if(outputFormat === 'ProRes 4444') {
                    outputFormat = '4'
                }
                else if(outputFormat === 'ProRes 4444 XQ') {
                    outputFormat = '5'
                }

                if(resizeX && (resizeX !== 'none' && (parseInt(resizeX) > 9999 || parseInt(resizeX) < 100 ))) {
                    await reportErr( new Error("ResizeX: Invalid value provided. Must be a number between 100 and 9999."))
                }

                if(resizeY && (resizeY !== 'none' && (parseInt(resizeY) > 9999 || parseInt(resizeY) < 100 ))) {
                    await reportErr( new Error("ResizeY: Invalid value provided. Must be a number between 100 and 9999."))
                }

                let outputFile = Utils.generateEFSOutputFile(SERVICE, PARAMS, INDEX, 'transcodeUrl', transcodeVideo.extension);
                params.serviceType = SERVICE_TYPES.TRANSCODE;
                console.log('resizeY', resizeY)
                params.serviceInfos = {
                    outputExtension: SERVICE.output,
                    fileName: outputFile.fileName,
                    outDir: outputFile.filePath,
                    action: ACTIONS.TRANSCODE,
                    linkToFile: `${PARAMS.workflowExcutionId}/${transcodeVideo.efsLocation}`,
                    timeCode: SERVICE.timeCode_value ? SERVICE.timeCode_value : SERVICE.timeCode,
                    videoBitrate: SERVICE.videoBitrate_value ? SERVICE.videoBitrate_value : SERVICE.videoBitrate,
                    prcodec: outputFormat,
                    colorSpace: colorSpace,
                    gammaCurve: gammaCurve,
                    resizeX: resizeX,
                    resizeY: resizeY,
                }
                break;

            default:
                throw new Error('Action is not supported');
        }

        const requestOptions = {
            uri: REDLINE_ENDPOINT,
            method: 'POST',
            headers: { ApiKey: TOKEN },
            body: params,
            json: true
        }
        console.log('Request FFmpeg transcode: ', requestOptions);
        return RP(requestOptions).then(res => {
            if (res.data) return res.data;
            else throw "No transcode Job"
        }).catch(err => {
            console.log('Request trancoder error: ', err);
            throw err;
        })
    } catch (err) {
        console.log('CREATE TRANSCODE JOB ERROR: ', err);
        throw err;
    }
}

const processing = async (jobID) => {
    var countUp = 0;
    var isDone = false;
    var output = null;
    let transcodeStatus = null;
    try {
        while (isDone == false) {
            transcodeStatus = await checkTranscodeStatus(jobID);
            console.log('processing status: ', transcodeStatus);
            if (transcodeStatus && transcodeStatus.data) {
                transcodeStatus = transcodeStatus.data;
                switch (transcodeStatus.status) {
                    case "SUBMITTED":
                    case "PROGRESSING":
                        if (countUp >= TIME_OUT) throw new Error("Transcode time out.");
                        isDone = false;
                        break;
                    case "COMPLETE":
                        isDone = true;
                        output = transcodeStatus.responseData;
                        break;
                    case "CANCELED":
                        isDone = true;
                        break;
                    case "ERROR":
                        throw new Error(transcodeStatus.message);
                }
            }
            if (isDone) break;
            countUp += GET_STATUS_INTERVAL;
            await Utils.sleep(GET_STATUS_INTERVAL);
        }
        return output;
    } catch (err) {
        throw err;
    }
}

const checkTranscodeStatus = async (jobId) => {
    console.log('Check transcode status: ', jobId);
    var endpoint = REDLINE_ENDPOINT + '/' + jobId;
    const statusOptions = {
        uri: endpoint,
        method: 'GET',
        headers: {
            ApiKey: TOKEN
        },
        json: true
    }
    return RP(statusOptions).then(res => {
        console.log('FFmpeg transcode status: ', res);
        return res;
    });
}

async function initService() {
    SERVICE = await Utils.getServiceEnv(INDEX, PARAMS);
    console.log('SERVICE', typeof SERVICE, SERVICE);
    console.log('PARAMS', PARAMS);

    // CHANGE WORKING DIRECTORY
    fs.mkdirSync(`./${PARAMS.workflowExcutionId}/${SERVICE.name}`, { recursive: true });
    process.chdir(`./${PARAMS.workflowExcutionId}`);
    console.log('WORKING DIR', process.cwd());

    RESET_TIME = START_TIME = moment();
    INTERVAL = setInterval(() => { intervalCharge(START_TIME.valueOf().toString(), SERVICE.name, PARAMS, PAYMENT_STATUS.IN_PROGRESS, INTERVAL_TIME) }, INTERVAL_TIME);
}

(async () => {
    try {
        await initService();
        await redlineTranscode();
    } catch (err) {
        console.error('Rejection handled.', err);
        await reportErr(err)
    }
})();