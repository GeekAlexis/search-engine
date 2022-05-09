import React from 'react';
import Search from './Search';
import '../styles/Index.css';

function Index() {
  return (
    <>
      <div className="index">
        <div className="logo">
          <img src="/logo.png" alt="logo" />
        </div>
        <Search />
      </div>
    </>
  );
}

export default Index;
