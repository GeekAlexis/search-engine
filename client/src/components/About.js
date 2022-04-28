import React, { useState, useEffect } from 'react';
import Box from '@mui/material/Box';
import Popper from '@mui/material/Popper';
import Typography from '@mui/material/Typography';
import Grid from '@mui/material/Grid';
import Button from '@mui/material/Button';
import Fade from '@mui/material/Fade';
import Paper from '@mui/material/Paper';
import ClickAwayListener from '@mui/material/ClickAwayListener';
import PropTypes from 'prop-types';

function About(props) {
  const { open, setOpen, anchorEl } = props;
  function handleClick() {
    setOpen((prev) => !prev);
  }

  function handleClickAway() {
    setOpen(false);
  }

  return (
    <Popper open={open} anchorEl={anchorEl} placement="right" transition>
      {({ TransitionProps }) => (
        <ClickAwayListener onClickAway={handleClickAway}>
          <Fade {...TransitionProps} timeout={350}>
            <Paper>
              <Typography sx={{ p: 2 }}>Placeholder</Typography>
            </Paper>
          </Fade>
        </ClickAwayListener>
      )}
    </Popper>
  );
}

About.propTypes = {
  open: PropTypes.bool,
  setOpen: PropTypes.func,
  anchorEl: PropTypes.object,
};

export default About;
