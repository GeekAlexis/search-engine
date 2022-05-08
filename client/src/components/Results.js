import React, { useState, useEffect, useRef } from 'react';
import Pagination from '@mui/material/Pagination';
import Record from './Record';
import LinearProgress from '@mui/material/LinearProgress';
import PropTypes from 'prop-types';
import getSearchResults from '../apis/getSearchResults';

function Results(props) {
  const { query } = props;
  const [loading, setLoading] = useState(false);

  const [page, setPage] = useState(1);
  const pageSize = 10;
  const [pageCount, setPageCount] = useState(0);
  const [items, setItems] = useState([]);

  const prevQuery = useRef(query);

  async function fetch() {
    console.log(`fetching page ${page} for query ${query}`);
    setLoading(true);
    const data = await getSearchResults(query, page);
    if (data) {
      setPageCount(Math.ceil(data.match / pageSize));
      setItems(data.data);
    } else {
      setPageCount(0);
      setItems([]);
    }
    setLoading(false);
  }

  useEffect(() => {
    setPage(1);
  }, [query]);

  useEffect(() => {
    if (query !== prevQuery.current && page != 1) {
      prevQuery.current = query;
    } else {
      fetch();
    }
  }, [query, page]);

  function handleChange(e, p) {
    setPage(p);
  }

  return (
    <React.Fragment>
      {loading ? (
        <LinearProgress />
      ) : (
        <React.Fragment>
          {items.map((record, i) => (
            <Record key={i} data={record} />
          ))}

          <Pagination
            className="pagination"
            count={pageCount}
            page={page}
            onChange={handleChange}
          />
        </React.Fragment>
      )}
    </React.Fragment>
  );
}

Results.propTypes = {
  query: PropTypes.string,
};

export default Results;
