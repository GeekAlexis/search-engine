import React, { useState, useEffect } from 'react';
import { styled } from '@mui/material/styles';
import Tabs from '@mui/material/Tabs';
import Tab from '@mui/material/Tab';
import Box from '@mui/material/Box';
import TabPanel from './TabPanel';
import SearchIcon from '@mui/icons-material/Search';
import NewspaperIcon from '@mui/icons-material/Newspaper';
import StoreIcon from '@mui/icons-material/Store';
import Grid from '@mui/material/Grid';
import Results from './Results';
import News from './News';
import Businesses from './Businesses';
import PropTypes from 'prop-types';
import '../styles/Options.css';

function Options(props) {
  const { q } = props;
  const [value, setValue] = useState(0);

  const handleChange = (e, value) => {
    setValue(value);
  };

  const MyTab = styled(Tab)({
    textTransform: 'none',
    fontSize: '1rem',
  });

  return (
    <Box sx={{ width: '100%' }}>
      <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
        <Tabs value={value} onChange={handleChange}>
          <MyTab
            icon={<SearchIcon />}
            iconPosition="start"
            label="All"
            id="tab-1"
          />
          <MyTab
            icon={<NewspaperIcon />}
            iconPosition="start"
            label="News"
            id="tab-2"
          />
          <MyTab
            icon={<StoreIcon />}
            iconPosition="start"
            label="Businesses"
            id="tab-3"
          />
        </Tabs>
      </Box>
      <TabPanel value={value} index={0}>
        <Grid container spacing={2}>
          <Grid className="results" item xs={6}>
            <Results query={q} />
          </Grid>
          <Grid item xs={6}></Grid>
        </Grid>
      </TabPanel>
      <TabPanel value={value} index={1}>
        <Grid container spacing={2}>
          <Grid className="news" item xs={6}>
            <News query={q} />
          </Grid>
          <Grid item xs={6}></Grid>
        </Grid>
      </TabPanel>
      <TabPanel value={value} index={2}>
        <Grid container spacing={2}>
          <Grid className="businesses" item xs={9}>
            <Businesses query={q} />
          </Grid>
          <Grid item xs={3}></Grid>
        </Grid>
      </TabPanel>
    </Box>
  );
}

Options.propTypes = {
  q: PropTypes.string,
};

export default Options;
