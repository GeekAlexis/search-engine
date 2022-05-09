const axios = require('axios');

async function getSearchResults(query, page) {
  if (query == null || query.length == 0) {
    return { match: 0, data: [] };
  }
  try {
    const res = await axios.get(
      `http://3.239.191.178:4567/search?query=${query}&page=${page}`
    );
    if (res.status === 200) {
      return res.data;
    }
  } catch (err) {
    console.error(err);
  }
}

export default getSearchResults;
