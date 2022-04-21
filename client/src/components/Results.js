import React, { useState, useEffect } from "react";
import { useSearchParams } from "react-router-dom";
import Search from "./Search";
import Record from "./Record";

const data = {
  url: "https://en.wikipedia.org/wiki/Search_engine",
  baseUrl: "https://en.wikipedia.org",
  path: "> wiki > Search_engine",
  title: "Search engine - Wikipedia",
  excerpt:
    "A search engine is a software system that is designed to carry out web searches. They search the World Wide Web in a systematic way for particular ...",
};

function Results() {
  const [searchParams, setSearchParams] = useSearchParams();
  const q = searchParams.get("q");

  return (
    <>
      <Search q={q} />
      <Record data={data} />
    </>
  );
}

export default Results;
