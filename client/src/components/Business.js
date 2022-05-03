import React, { useState, useEffect } from 'react';
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import CardMedia from '@mui/material/CardMedia';
import { CardActionArea } from '@mui/material';
import Grid from '@mui/material/Grid';
import Rating from '@mui/material/Rating';
import StarIcon from '@mui/icons-material/Star';
import Typography from '@mui/material/Typography';
import PropTypes from 'prop-types';
import '../styles/Business.css';

function Business(props) {
  const { data } = props;

  return (
    <Card
      className="business"
      variant="outlined"
      sx={{ maxWidth: 240 }}
      style={{ boxShadow: 'none' }}
    >
      <CardActionArea href={data.url} target="_blank">
        <CardMedia
          component="img"
          height="160"
          image={data.image_url}
          alt={data.alias}
        />
        <CardContent>
          <Typography
            gutterBottom
            variant="h5"
            component="div"
            sx={{
              fontSize: 19,
            }}
          >
            {data.name}
          </Typography>

          <Grid container spacing={1}>
            <Grid item xs={6}>
              <Rating
                name="rating"
                value={data.rating}
                readOnly
                precision={0.5}
                emptyIcon={
                  <StarIcon style={{ opacity: 0.55 }} fontSize="inherit" />
                }
                size="small"
              />
            </Grid>

            <Grid item xs={6}>
              <Typography
                variant="body2"
                color="text.secondary"
                sx={{
                  fontSize: 12,
                }}
              >
                {data.review_count} reviews
              </Typography>
            </Grid>
          </Grid>

          <Typography
            variant="subtitle2"
            sx={{
              display: 'inline-block',
              fontSize: 12.75,
              lineHeight: 1.35,
            }}
          >
            {data.price == null ? '' : data.price + ' · '}
            {data.categories.map((category) => category.title).join(', ')}
          </Typography>

          <Typography
            variant="subtitle2"
            sx={{
              display: 'block',
              fontSize: 12,
              lineHeight: 1.25,
              marginTop: 0.25,
            }}
          >
            <span className="span">Service options: </span>
            {data.transactions.map((transaction) => transaction).join(' · ')}
          </Typography>

          <Typography
            variant="subtitle2"
            sx={{
              display: 'block',
              fontSize: 12,
              lineHeight: 1.25,
              marginTop: 0.25,
            }}
          >
            <span className="span">Address: </span>
            {data.location.display_address.join(', ')}
          </Typography>

          <Typography
            variant="subtitle2"
            sx={{ fontSize: 12, lineHeight: 1.25, marginTop: 0.25 }}
          >
            <span className="span">Phone: </span>
            {data.display_phone}
          </Typography>
        </CardContent>
      </CardActionArea>
    </Card>
  );
}

Business.propTypes = {
  data: PropTypes.object,
};

export default Business;
