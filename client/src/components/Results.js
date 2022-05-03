import React, { useState, useEffect } from 'react';
import Pagination from '@mui/material/Pagination';
import Record from './Record';
import PropTypes from 'prop-types';
import getSearchResults from '../apis/getSearchResults';

/*
const record = {
  url: 'https://en.wikipedia.org/wiki/Search_engine',
  baseUrl: 'https://en.wikipedia.org',
  path: '› wiki › Search_engine',
  title: 'Search engine - Wikipedia',
  excerpt:
    'A <span>search</span> engine is a software system that is designed to carry out web searches. They search the World Wide Web in a systematic way for particular ...',
};

const data = Array(100).fill(record);
*/

function Results(props) {
  const { query } = props;
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);

  const [page, setPage] = useState(1);
  const pageSize = 10;
  const pageCount = Math.ceil(data.length / pageSize);
  const [items, setItems] = useState(data.slice(0, pageSize));

  useEffect(() => {
    async function fetch() {
      setLoading(true);
      const data = await getSearchResults(query);
      setData(data);
      setLoading(false);
      setItems(data.slice(0, pageSize));
    }
    fetch();
  }, [query]);

  function handleChange(e, p) {
    setPage(p);
    setItems(data.slice((p - 1) * pageSize, p * pageSize));
  }

  return (
    <>
      {items.map((record, i) => (
        <Record key={i} data={record} />
      ))}

      <Pagination
        className="pagination"
        count={pageCount}
        page={page}
        onChange={handleChange}
      />
    </>
  );
}

Results.propTypes = {
  query: PropTypes.string,
};

export default Results;
