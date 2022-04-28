import React, { useState, useEffect } from "react";
import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";
import Box from "@mui/material/Box";
import TabPanel from "./TabPanel";
import SearchIcon from "@mui/icons-material/Search";
import NewspaperIcon from "@mui/icons-material/Newspaper";
import StoreIcon from "@mui/icons-material/Store";
import Grid from "@mui/material/Grid";
import Results from "./Results";
import News from "./News";
import PropTypes from "prop-types";

function Options(props) {
  const { q } = props;
  const [value, setValue] = useState(0);

  const handleChange = (e, value) => {
    setValue(value);
  };

  return (
    <Box sx={{ width: "100%" }}>
      <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
        <Tabs value={value} onChange={handleChange}>
          <Tab
            icon={<SearchIcon />}
            iconPosition="start"
            label="All"
            id="tab-1"
          />
          <Tab
            icon={<NewspaperIcon />}
            iconPosition="start"
            label="News"
            id="tab-2"
          />
          <Tab
            icon={<StoreIcon />}
            iconPosition="start"
            label="Shopping"
            id="tab-3"
          />
        </Tabs>
      </Box>
      <TabPanel value={value} index={0}>
        <Grid container spacing={2}>
          <Grid className="results" item xs={6}>
            <Results />
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
        Shopping
      </TabPanel>
    </Box>
  );
}

Options.propTypes = {
  q: PropTypes.string,
};

export default Options;
