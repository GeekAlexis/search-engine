import React, { useState, useEffect } from "react";
import Box from "@mui/material/Box";
import TextField from "@mui/material/TextField";
import Paper from "@mui/material/Paper";
import InputBase from "@mui/material/InputBase";
import Divider from "@mui/material/Divider";
import IconButton from "@mui/material/IconButton";
import SearchIcon from "@mui/icons-material/Search";

function Search() {
  const [query, setQuery] = useState("");

  return (
    <Box
      sx={{
        width: 600,
        maxWidth: "100%",
      }}
    >
      <TextField fullWidth id="input" />
    </Box>
  );
}

export default Search;
