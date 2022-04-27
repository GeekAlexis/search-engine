import React, { useState, useEffect } from "react";
import Pagination from "@mui/material/Pagination";
import News from "./News";
import PropTypes from "prop-types";

const article = {
  source: {
    id: "the-verge",
    name: "The Verge",
  },
  author: "Jay Peters",
  title:
    "Block and Blockstream are partnering with Tesla on an off-grid, solar-powered Bitcoin mine in Texas",
  description:
    "Block and Blockstream are partnering with Tesla on an open-source, solar-powered Bitcoin mine, the companies announced Friday. Tesla’s 3.8-megawatt Solar PV array and its 12 megawatt-hour Megapack will power the facility, and construction has started on the p…",
  url: "https://www.theverge.com/2022/4/8/23016553/block-blockstream-tesla-solar-bitcoin-mine-texas",
  urlToImage:
    "https://cdn.vox-cdn.com/thumbor/OYrvaaOHBuEpdTeRO55nZnZdexs=/0x215:3000x1786/fit-in/1200x630/cdn.vox-cdn.com/uploads/chorus_asset/file/8937281/acastro_170726_1777_0007_v2.jpg",
  publishedAt: "2022-04-08T16:02:52Z",
  content:
    "Its set to open later this year\r\nIf you buy something from a Verge link, Vox Media may earn a commission. See our ethics statement.\r\nIllustration by Alex Castro / The Verge\r\nBlock and Blockstream, a … [+1336 chars]",
};

const data = Array(100).fill(article);

function NewsPage(props) {
  const { query } = props;

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
      {items.map((article, i) => (
        <News key={i} data={article} />
      ))}

      <Pagination count={pageCount} page={page} onChange={handleChange} />
    </>
  );
}

NewsPage.propTypes = {
  query: PropTypes.string,
};

export default NewsPage;
