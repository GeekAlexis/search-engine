import React, { useState, useEffect } from 'react';
import Pagination from '@mui/material/Pagination';
import Record from './Record';
import PropTypes from 'prop-types';

const record = {
  url: 'https://en.wikipedia.org/wiki/Search_engine',
  baseUrl: 'https://en.wikipedia.org',
  path: '› wiki › Search_engine',
  title: 'Search engine - Wikipedia',
  excerpt:
    'A search engine is a software system that is designed to carry out web searches. They search the World Wide Web in a systematic way for particular ...',
};

const data = Array(100).fill(record);

function Results(props) {
  // const { data } = props;
  const [page, setPage] = useState(1);
  const pageSize = 10;
  const pageCount = Math.ceil(data.length / pageSize);
  const [items, setItems] = useState(data.slice(0, pageSize));

  function handleChange(e, p) {
    setPage(p);
    setItems(data.slice((p - 1) * pageSize, p * pageSize));
  }

  return (
    <>
      {items.map((record, i) => (
        <Record key={i} data={record} />
      ))}

      <Pagination count={pageCount} page={page} onChange={handleChange} />
    </>
  );
}

Results.propTypes = {
  data: PropTypes.array,
};

export default Results;
