const axios = require('axios');

async function getNews(query) {
  try {
    const res = await axios.get(
      `http://54.160.45.131:4567/news?query=${query}`
    );
    if (res.status === 200) {
      return res.data.articles;
    }
  } catch (err) {
    console.error(err);
  }
  return [];
}

export default getNews;
