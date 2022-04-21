import React, { useState, useEffect } from "react";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardActions from "@mui/material/CardActions";
import CardContent from "@mui/material/CardContent";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import Link from "@mui/material/Link";
import PropTypes from "prop-types";

function Record(props) {
  const { data } = props;

  return (
    <Card sx={{ maxWidth: "50%" }} style={{ boxShadow: "none" }}>
      <CardContent>
        <Typography display="inline" sx={{ mb: 1.5 }}>
          {data.baseUrl}&nbsp;
        </Typography>
        <Typography display="inline" sx={{ mb: 1.5 }} color="text.secondary">
          {data.path}
        </Typography>
        <br />
        <Link
          href={data.url}
          variant="h5"
          underline="hover"
          target="_blank"
          rel="noopener noreferrer"
        >
          {data.title}
        </Link>
        <Typography variant="body2">{data.excerpt}</Typography>
      </CardContent>
    </Card>
  );
}

Record.propTypes = {
  data: PropTypes.object,
};

export default Record;
