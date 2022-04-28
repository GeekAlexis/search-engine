import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import Paper from '@mui/material/Paper';
import Popper from '@mui/material/Popper';
import InputBase from '@mui/material/InputBase';
import IconButton from '@mui/material/IconButton';
import SearchIcon from '@mui/icons-material/Search';
import Autocomplete, { createFilterOptions } from '@mui/material/Autocomplete';
import PropTypes from 'prop-types';
import '../styles/Search.css';
import { topQueries } from '../data/topQueries';

function Search(props) {
  const [query, setQuery] = useState('');

  const navigate = useNavigate();
  const { q } = props;

  const filterOptions = createFilterOptions({
    matchFrom: 'start',
    limit: 10,
  });

  const CustomPopper = function (props) {
    return (
      <Popper
        {...props}
        style={{
          width: '484px',
          paddingTop: '10px',
        }}
        placement="bottom-start"
      />
    );
  };

  function handleChange(e, value) {
    setQuery(e.target.value);
  }

  function handleEnter(e) {
    if (e.key === 'Enter') {
      e.preventDefault();
      if (query.length > 0) {
        console.log('Enter pressed with query: ', query);
        navigate('/search?q=' + query);
      }
    }
  }

  function handleSearch() {
    if (query.length > 0) {
      console.log('Search clicked with query: ', query);
      navigate('/search?q=' + query);
    }
  }

  function handleClick(e, value) {
    if (value) {
      console.log('Option selected with query: ', value);
      navigate('/search?q=' + value);
    }
  }

  return (
    <div className="search">
      <Paper
        component="form"
        sx={{
          p: '2px 4px',
          display: 'flex',
          alignItems: 'center',
          width: 500,
        }}
      >
        <Autocomplete
          freeSolo
          defaultValue={q}
          PopperComponent={CustomPopper}
          filterOptions={filterOptions}
          onChange={handleClick}
          options={topQueries}
          style={{ width: 600 }}
          renderOption={(props, option) => (
            <li {...props}>
              <IconButton>
                <SearchIcon />
              </IconButton>
              {option}
            </li>
          )}
          renderInput={(params) => (
            <InputBase
              ref={params.InputProps.ref}
              inputProps={params.inputProps}
              sx={{ ml: 1, flex: 1 }}
              placeholder="Search"
              onChange={handleChange}
              onKeyDown={handleEnter}
            />
          )}
        />
        <IconButton
          className="search-icon"
          type="submit"
          sx={{ p: '10px' }}
          onClick={handleSearch}
        >
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
