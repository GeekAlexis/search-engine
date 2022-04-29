import React, { useState, useEffect } from 'react';
import Grid from '@mui/material/Grid';
import Pagination from '@mui/material/Pagination';
import LinearProgress from '@mui/material/LinearProgress';
import Business from './Business';
import PropTypes from 'prop-types';
import getBusinesses from '../apis/getBusinesses';

function Businesses(props) {
  const { query } = props;
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(false);

  const [page, setPage] = useState(1);
  const pageSize = 12;
  const pageCount = Math.ceil(data.length / pageSize);
  const [items, setItems] = useState([]);

  useEffect(() => {
    async function fetch() {
      setLoading(true);
      const data = await getBusinesses(query);
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
    <React.Fragment>
      {loading ? (
        <LinearProgress />
      ) : (
        <React.Fragment>
          <Grid container spacing={2}>
            {items.map((business, i) => (
              <Grid key={i} item xs={3}>
                <Business data={business} />
              </Grid>
            ))}
          </Grid>

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

Businesses.propTypes = {
  query: PropTypes.string,
};

export default Businesses;
