import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import Paper from "@mui/material/Paper";
import InputBase from "@mui/material/InputBase";
import IconButton from "@mui/material/IconButton";
import SearchIcon from "@mui/icons-material/Search";
import PropTypes from "prop-types";
import "../styles/Search.css";

function Search(props) {
  const [query, setQuery] = useState("");
  const navigate = useNavigate();
  const { q } = props;

  function handleChange(e) {
    setQuery(e.target.value);
  }

  function handleEnter(e) {
    if (e.key === "Enter") {
      if (query.length > 0) {
        navigate("/search?q=" + query);
      }
    }
  }

  function handleSearch() {
    if (query.length > 0) {
      navigate("/search?q=" + query);
    }
  }

  return (
    <div className="search">
      <Paper
        component="form"
        sx={{
          p: "2px 4px",
          display: "flex",
          alignItems: "center",
          width: 500,
        }}
      >
        <InputBase
          sx={{ ml: 1, flex: 1 }}
          placeholder="Search"
          onChange={handleChange}
          onKeyDown={handleEnter}
          defaultValue={q}
        />
        <IconButton type="submit" sx={{ p: "10px" }} onClick={handleSearch}>
          <SearchIcon />
        </IconButton>
      </Paper>
    </div>
  );
}

Search.propTypes = {
  q: PropTypes.string,
};

export default Search;
