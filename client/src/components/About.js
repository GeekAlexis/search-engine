import React, { useState, useEffect } from 'react';
import Backdrop from '@mui/material/Backdrop';
import Box from '@mui/material/Box';
import Popper from '@mui/material/Popper';
import Typography from '@mui/material/Typography';
import Grid from '@mui/material/Grid';
import Button from '@mui/material/Button';
import Fade from '@mui/material/Fade';
import Paper from '@mui/material/Paper';
import Divider from '@mui/material/Divider';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogContentText from '@mui/material/DialogContentText';
import DialogTitle from '@mui/material/DialogTitle';
import ClickAwayListener from '@mui/material/ClickAwayListener';
import PropTypes from 'prop-types';

function About(props) {
  const { open, setOpen, anchorEl, data } = props;

  function handleClickAway() {
    setOpen(false);
  }

  function handleClose() {
    setOpen(false);
  }

  return (
    <React.Fragment>
      <Backdrop open={open} onClick={handleClose} />
      <Popper
        className="about"
        open={open}
        anchorEl={anchorEl}
        placement="right"
        transition
      >
        {({ TransitionProps }) => (
          <ClickAwayListener onClickAway={handleClickAway}>
            <Fade {...TransitionProps} timeout={350}>
              <Paper>
                <Box className="content">
                  <Typography variant="h6">About this result</Typography>
                  <Divider />
                  <Typography variant="body2">BM25: {data.bm25}</Typography>
                  <Typography variant="body2">
                    PageRank: {data.pageRank}
                  </Typography>
                  <Typography variant="body2">Score: {data.score}</Typography>
                </Box>
              </Paper>
            </Fade>
          </ClickAwayListener>
        )}
      </Popper>
    </React.Fragment>
  );
}

About.propTypes = {
  open: PropTypes.bool,
  setOpen: PropTypes.func,
  anchorEl: PropTypes.object,
  data: PropTypes.object,
};

export default About;
