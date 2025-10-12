// 공용 AJAX 에러 처리: 인증 만료 시 로그인 페이지로 이동
(function () {
  if (typeof window === 'undefined' || typeof window.jQuery === 'undefined') {
    return;
  }

  var LOGIN_PATH = '/cs/login';

  $(document).ajaxError(function (_event, jqXHR) {
    if (!jqXHR) return;

    // 401 또는 403 → 로그인 페이지로 이동
    if (jqXHR.status === 401 || jqXHR.status === 403) {
      // 이미 로그인 페이지면 무한 리다이렉트 방지
      if (window.location.pathname !== LOGIN_PATH) {
        window.location.href = LOGIN_PATH;
      }
    }
  });
})();

