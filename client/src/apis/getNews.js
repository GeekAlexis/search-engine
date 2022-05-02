const axios = require('axios');

async function getNews(query) {
  try {
    const res = await axios.get(`/news?query=${query}`);
    if (res.status === 200) {
      return res.data.articles;
    }
  } catch (err) {
    console.error(err);
  }
}

export default getNews;
