import React, { useState, useEffect } from "react";
import Pagination from "@mui/material/Pagination";
import LinearProgress from "@mui/material/LinearProgress";
import Article from "./Article";
import PropTypes from "prop-types";
import getNews from "../apis/getNews";

function News(props) {
  const { query } = props;
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);

  const [page, setPage] = useState(1);
  const pageSize = 10;
  const pageCount = Math.ceil(data.length / pageSize);
  const [items, setItems] = useState([]);

  useEffect(() => {
    async function fetch() {
      setLoading(true);
      const data = await getNews(query);
      setData(data);
      setLoading(false);
      setItems(data.slice(0, pageSize));
    }
    fetch();
  }, []);

  function handleChange(e, p) {
    setPage(p);
    setItems(data.slice((p - 1) * pageSize, p * pageSize));
  }

  return (
    <>
      {loading ? (
        <LinearProgress />
      ) : (
        <>
          {items.map((article, i) => (
            <Article key={i} data={article} />
          ))}

          <Pagination count={pageCount} page={page} onChange={handleChange} />
        </>
      )}
    </>
  );
}

News.propTypes = {
  query: PropTypes.string,
};

export default News;
