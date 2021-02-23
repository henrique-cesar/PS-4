const axios = require('axios');
const access_token = 'token-tt-thingsboard';

const api = axios.default.create({
    baseURL: 'http://54.90.25.87:8080/api/v1'
})


exports.handler = async (event) => {
	const data = event.Records;
	let filteredData = {
		updatedAt: getHour()
	};
	
	for (let e of data) {
			let obj = JSON.parse(Buffer.from(e.kinesis.data, 'base64').toString('utf8'));
			filteredData[obj.DISTRICT] = obj.HEAT_INDEX;
	}
	
	console.log(filteredData);
	await api.post(`/${access_token}/telemetry`, filteredData);
	
	const response = {
			statusCode: 200,
			body: JSON.stringify(filteredData),
	};
	return response;
};


function getHour() {
    var date = new Date();

    var hour = date.getHours();
    
    if ((hour-3) < 0) {
    	hour = (hour - 3) + 24;
    }
    else {
    	hour = hour - 3;
    }
    
    hour = (hour < 10 ? "0" : "") + hour;

    var min  = date.getMinutes();
    min = (min < 10 ? "0" : "") + min;
    
    return hour + ":" + min;
}
