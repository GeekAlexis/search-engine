import React, { useState, useEffect } from "react";
import Pagination from "@mui/material/Pagination";
import Record from "./Record";
import PropTypes from "prop-types";

function Results(props) {
  const { data } = props;
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
