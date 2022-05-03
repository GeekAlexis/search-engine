const axios = require('axios');

async function getSearchResults(query) {
  /*
  const record = {
    bm25: 0.3,
    excerpt: `Excerpt for <span>'${query}'</span> Excerpt for <span>'${query}'</span> Excerpt for <span>'${query}'</span>...`,
    pageRank: 0.333333,
    score: 0.6666666,
    title: 'Search engine - Wikipedia',
    url: 'https://en.wikipedia.org/wiki/Search_engine',
    baseUrl: 'https://en.wikipedia.org',
    path: '› wiki › Search_engine',
  };

  const res = { data: Array(100).fill(record) };
  return res.data;
  */

  try {
    const res = await axios.get(`/search?query=${query}`);
    if (res.status === 200) {
      return res.data;
    }
  } catch (err) {
    console.error(err);
  }
}

export default getSearchResults;
