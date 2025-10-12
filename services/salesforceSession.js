let tokenCache = null;

function setToken(token) {
  tokenCache = token;
}

function getToken() {
  return tokenCache;
}

module.exports = {
  setToken,
  getToken,
};

