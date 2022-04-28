import React, { useState, useEffect } from 'react';
import Box from '@mui/material/Box';
import Card from '@mui/material/Card';
import CardActions from '@mui/material/CardActions';
import CardContent from '@mui/material/CardContent';
import Button from '@mui/material/Button';
import Typography from '@mui/material/Typography';
import Link from '@mui/material/Link';
import IconButton from '@mui/material/IconButton';
import MoreVertIcon from '@mui/icons-material/MoreVert';
import About from './About';
import PropTypes from 'prop-types';
import '../styles/Record.css';

function Record(props) {
  const { data } = props;
  const [open, setOpen] = useState(false);
  const [anchorEl, setAnchorEl] = useState(null);

  function handleOpen(e) {
    setOpen(true);
    setAnchorEl(e.currentTarget.parentNode);
  }

  return (
    <Card style={{ boxShadow: 'none' }}>
      <CardContent>
        <Typography display="inline" sx={{ mb: 1.5 }}>
          {data.baseUrl}&nbsp;
        </Typography>
        <Typography display="inline" sx={{ mb: 1.5 }} color="text.secondary">
          {data.path}
        </Typography>

        <IconButton className="more-icon" size="small" onClick={handleOpen}>
          <MoreVertIcon />
        </IconButton>
        <About open={open} setOpen={setOpen} anchorEl={anchorEl} />

        <br />
        <Link
          href={data.url}
          style={{ color: '#1a0dab' }}
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
