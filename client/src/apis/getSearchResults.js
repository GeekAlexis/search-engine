const axios = require('axios');

async function getSearchResults(query, page) {
  try {
    const res = await axios.get(`/search?query=${query}&page=${page}`);
    if (res.status === 200) {
      return res.data;
    }
  } catch (err) {
    console.error(err);
  }
}

export default getSearchResults;
