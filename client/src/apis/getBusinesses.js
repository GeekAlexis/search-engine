const axios = require('axios');

async function getBusinesses(query) {
  const location = '19104';
  try {
    const res = await axios.get(`/yelp/${query}/${location}`);
    if (res.status === 200) {
      return res.data.businesses;
    }
  } catch (err) {
    console.error(err);
  }
  return [];
}

export default getBusinesses;
