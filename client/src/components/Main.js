import React, { useState, useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import Grid from "@mui/material/Grid";
import Search from "./Search";
import Results from "./Results";
import Record from "./Record";

const record = {
  url: "https://en.wikipedia.org/wiki/Search_engine",
  baseUrl: "https://en.wikipedia.org",
  path: "> wiki > Search_engine",
  title: "Search engine - Wikipedia",
  excerpt:
    "A search engine is a software system that is designed to carry out web searches. They search the World Wide Web in a systematic way for particular ...",
};

const data = Array(100).fill(record);

function Main() {
  const [searchParams, setSearchParams] = useSearchParams();
  const q = searchParams.get("q");

  return (
    <Grid container spacing={2}>
      <Grid item xs={8}>
        <Search q={q} />
      </Grid>
      <Grid item xs={4}></Grid>
      <Grid item xs={6}>
        <Results data={data} />
      </Grid>
      <Grid item xs={6}></Grid>
    </Grid>
  );
}

export default Main;
