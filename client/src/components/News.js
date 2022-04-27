import React, { useState, useEffect } from "react";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardActions from "@mui/material/CardActions";
import CardContent from "@mui/material/CardContent";
import CardMedia from "@mui/material/CardMedia";
import { CardActionArea } from "@mui/material";
import Grid from "@mui/material/Grid";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import Link from "@mui/material/Link";
import PropTypes from "prop-types";

function News(props) {
  const { data } = props;

  return (
    <Card
      variant="outlined"
      sx={{ maxWidth: 600 }}
      style={{ boxShadow: "none" }}
    >
      <CardActionArea href={data.url} target="_blank">
        <Grid container spacing={0}>
          <Grid item xs={9}>
            <CardContent>
              <Typography variant="subtitle2" gutterBottom component="div">
                {data.source.name}
              </Typography>
              <Typography
                sx={{ lineHeight: 1.125 }}
                gutterBottom
                variant="h6"
                component="div"
              >
                {data.title}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {data.description.length > 140
                  ? data.description.substring(0, 140) + "..."
                  : data.description}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {data.publishedAt}
              </Typography>
            </CardContent>
          </Grid>

          <Grid item xs={3}>
            <CardMedia
              className="news-img"
              component="img"
              height="60%"
              image={data.urlToImage}
            />
          </Grid>
        </Grid>
      </CardActionArea>
    </Card>
  );
}

News.propTypes = {
  data: PropTypes.object,
};

export default News;
