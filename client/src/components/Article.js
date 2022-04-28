import React from 'react';
import Card from '@mui/material/Card';
import CardContent from '@mui/material/CardContent';
import CardMedia from '@mui/material/CardMedia';
import { CardActionArea } from '@mui/material';
import Grid from '@mui/material/Grid';
import Typography from '@mui/material/Typography';
import PropTypes from 'prop-types';
import '../styles/Article.css';

function Article(props) {
  const { data } = props;

  function formatTime(time) {
    const date = new Date(time);
    const options = {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    };
    return date.toLocaleString('en-US', options);
  }

  return (
    <Card
      className="article"
      variant="outlined"
      sx={{ maxWidth: 600 }}
      style={{ boxShadow: 'none' }}
    >
      <CardActionArea href={data.url} target="_blank">
        <Grid container spacing={0}>
          <Grid item xs={9}>
            <CardContent>
              <Typography variant="subtitle2" gutterBottom component="div">
                {data.source.name}
              </Typography>
              <Typography
                className="title"
                sx={{ lineHeight: 1.125 }}
                gutterBottom
                variant="h6"
                component="div"
              >
                {data.title}
              </Typography>
              <Typography variant="body" color="text.secondary">
                {data.description.length > 140
                  ? data.description.substring(0, 140) + '...'
                  : data.description}
              </Typography>
              <Typography
                className="date"
                variant="body2"
                color="text.secondary"
              >
                {formatTime(data.publishedAt)}
              </Typography>
            </CardContent>
          </Grid>

          <Grid item xs={3}>
            <CardMedia
              component="img"
              image={
                data.urlToImage
                  ? data.urlToImage
                  : 'https://upload.wikimedia.org/wikipedia/commons/thumb/d/da/Google_News_icon.svg/512px-Google_News_icon.svg.png'
              }
            />
          </Grid>
        </Grid>
      </CardActionArea>
    </Card>
  );
}

Article.propTypes = {
  data: PropTypes.object,
};

export default Article;
