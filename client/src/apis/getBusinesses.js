const axios = require('axios');

async function getBusinesses(query) {
  let location = '19104';
  try {
    const res1 = await axios.get('http://ip-api.com/json');
    if (res1.status == 200) {
      location = res1.data.zip;
    }
  } catch (err1) {
    console.error(err1);
  }

  try {
    const res2 = await axios.get(`/yelp/${query}/${location}`);
    if (res2.status === 200) {
      return res2.data.businesses;
    }
  } catch (err2) {
    console.error(err2);
  }

  return [];
}

export default getBusinesses;
