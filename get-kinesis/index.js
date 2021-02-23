const axios = require('axios');
const kinesis = require('./kinesis');

exports.handler = async (event, context, callback) => {
  console.log('LOADING handler');
  
  const done = (err, res) => callback(null, {
    statusCode: err ? '400' : '200',
    body: err || res,
    headers: {
      'Content-Type': 'application/json',
    },
  });
  
  let data = await sendRequest();
  kinesis.save(data);
  done(null, data);
};

function getDateStr(isoString) {
	return isoString.split("T")[0];
}

function getURL() {
	// EXEMPLO: https://apitempo.inmet.gov.br/estacao/dados/2021-02-07/1500
	let baseURL = "https://apitempo.inmet.gov.br/estacao/dados/%UTCdate/%UTChour";
	let actualDate = new Date();
	let UTCDate = getDateStr(actualDate.toISOString());
	let UTCHour = actualDate.getUTCHours().toString().padStart(2, "0").concat("00");

	let url = baseURL.replace("%UTCdate", UTCDate);
	url = url.replace("%UTChour", UTCHour);
	return url;
}

async function sendRequest() {
	const response = await axios.get(getURL())
		.then(response => {
			return sanitizeData(response.data);
		})
		.catch(error => {
			console.log(error);
			return null;
		});
	return response;
}

function sanitizeData(data) {
	let filteredData = data.filter(e => e.UF === 'PE' && e.TEM_INS != null && e.UMD_INS != null);
	filteredData = filteredData.map(e => ({
		LOCAL: e.DC_NOME,
		TEMP: parseFloat(e.TEM_INS),
		UMID: parseFloat(e.UMD_INS),
		MIN: parseFloat(e.TEM_MIN),
		MAX: parseFloat(e.TEM_MAX)
	}));
	return filteredData;
}
