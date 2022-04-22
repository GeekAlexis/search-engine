import React, { useState, useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import Box from "@mui/material/Box";
import Paper from "@mui/material/Paper";
import Grid from "@mui/material/Grid";
import Search from "./Search";
import Options from "./Options";
import Results from "./Results";
import Record from "./Record";
import "../styles/Main.css";

function Main() {
  const [searchParams, setSearchParams] = useSearchParams();
  const q = searchParams.get("q");

  return (
    <div className="main">
      <Grid container spacing={2}>
        <Grid className="logo" item xs={1}>
          <a href="/">
            <img src="/logo.png" alt="logo" />
          </a>
        </Grid>
        <Grid item xs={8}>
          <Search q={q} />
        </Grid>
        <Grid item xs={3}></Grid>
      </Grid>

      <div className="options">
        <Options q={q} />
      </div>

      {/*
      <Grid container spacing={2}>
        <Grid className="results" item xs={6}>
          <Results data={data} />
        </Grid>
        <Grid item xs={6}></Grid>
      </Grid>
      */}
    </div>
  );
}

export default Main;
