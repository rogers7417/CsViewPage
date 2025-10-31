(function (global) {
  if (!global) return;

  var gridCommon = {};
  var resizeCount = 0;

  gridCommon.API_BASE = "/cs/api";

  gridCommon.fmt = function (value) {
    var num = Number(value || 0);
    if (isNaN(num)) return "";
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  };

  gridCommon.unfmt = function (value) {
    if (value === null || value === undefined) return 0;
    return Number(String(value).replace(/[, %]/g, "")) || 0;
  };

  gridCommon.clamp = function (value, min, max) {
    return Math.min(Math.max(value, min), max);
  };

  gridCommon.labelKorean = function (ym) {
    if (!ym) return "";
    var parts = ym.split("-");
    return parts[0] + "년 " + Number(parts[1]) + "월";
  };

  gridCommon.addMonths = function (ym, delta) {
    var base = ym;
    if (!base || base.length < 7) {
      var today = new Date();
      today.setDate(1);
      base = today.toISOString().slice(0, 7);
    }
    var parts = base.split("-").map(Number);
    var date = new Date(Date.UTC(parts[0], parts[1] - 1, 1));
    date.setUTCMonth(date.getUTCMonth() + (delta || 0));
    return date.toISOString().slice(0, 7);
  };

  gridCommon.ensureMonthValue = function ($input) {
    if (!$input || !$input.length) return;
    var val = $input.val();
    if (val && val.length >= 7) return;
    var today = new Date();
    today.setDate(1);
    $input.val(today.toISOString().slice(0, 7));
  };

  gridCommon.initToolbar = function (options) {
    if (typeof global.jQuery === "undefined") return;

    var $ = global.jQuery;
    var opts = $.extend(
      {
        monthSelector: "#month",
        ownerSelector: "#ownerDept",
        loadSelector: "#loadBtn",
        prevSelector: "#prevBtn",
        nextSelector: "#nextBtn",
        autoLoad: true,
        onChange: null,
      },
      options || {}
    );

    var $month = $(opts.monthSelector);
    var $owner = $(opts.ownerSelector);
    var $load = $(opts.loadSelector);
    var $prev = $(opts.prevSelector);
    var $next = $(opts.nextSelector);

    gridCommon.ensureMonthValue($month);

    function emit() {
      if (typeof opts.onChange === "function") {
        opts.onChange();
      }
    }

    if ($load.length) {
      $load.off("click.gridCommon").on("click.gridCommon", emit);
    }
    if ($prev.length) {
      $prev.off("click.gridCommon").on("click.gridCommon", function () {
        gridCommon.ensureMonthValue($month);
        $month.val(gridCommon.addMonths($month.val(), -1));
        emit();
      });
    }
    if ($next.length) {
      $next.off("click.gridCommon").on("click.gridCommon", function () {
        gridCommon.ensureMonthValue($month);
        $month.val(gridCommon.addMonths($month.val(), 1));
        emit();
      });
    }
    if ($month.length) {
      $month.off("change.gridCommon").on("change.gridCommon", emit);
    }
    if ($owner.length) {
      $owner.off("change.gridCommon").on("change.gridCommon", emit);
    }

    if (opts.autoLoad) {
      emit();
    }
  };

  gridCommon.registerGridResize = function (selector, options) {
    if (typeof global.jQuery === "undefined") return function () {};
    var $ = global.jQuery;
    var settings = $.extend(
      {
        bottomOffset: 30,
        minHeight: 220,
      },
      options || {}
    );

    resizeCount += 1;
    var ns = ".gridCommon" + resizeCount;

    function adjust() {
      var $grid = $(selector);
      if (!$grid.length || !$grid[0].grid) return;

      var $wrapper = $grid.closest(".ui-jqgrid");
      if ($wrapper.length) {
        var maxWidth = $(selector).width() || $wrapper.parent().width();
        if (maxWidth) {
          $wrapper.css("max-width", maxWidth + "px");
          $wrapper.find(".ui-jqgrid-bdiv").css({
            overflowX: "auto",
          });
        }
      }

      var offset = $grid.offset();
      var top = offset ? offset.top : 0;
      var vh = global.innerHeight || $(global).height() || 0;
      var height = Math.max(settings.minHeight, vh - top - settings.bottomOffset);

      try {
        $grid.jqGrid("setGridHeight", height);
      } catch (err) {
        // ignore jqGrid errors
      }

      var $wrapper = $grid.closest(".ui-jqgrid").parent();
      var width = $wrapper.width();
      if (width) {
        try {
          $grid.jqGrid("setGridWidth", width);
        } catch (err2) {
          // ignore jqGrid errors
        }
      }
    }

    $(global).off("resize" + ns).on("resize" + ns, adjust);
    setTimeout(adjust, 0);
    return function cleanup() {
      $(global).off("resize" + ns, adjust);
    };
  };

  gridCommon.highlightActiveNav = function (selector) {
    if (typeof global.jQuery === "undefined") return;
    var $ = global.jQuery;
    var $root = $(selector);
    if (!$root.length) return;
    var current = global.location ? global.location.pathname.split("/").pop() : "";
    if (!current) current = "index.html";

    $root.find("a").each(function () {
      var $link = $(this);
      var href = ($link.attr("href") || "").split("/").pop();
      if (href === current) {
        $link.addClass("active");
      } else {
        $link.removeClass("active");
      }
    });
  };

  if (typeof global.jQuery !== "undefined") {
    global.jQuery(function () {
      gridCommon.highlightActiveNav(".sidebar__nav");
    });
  }

  global.gridCommon = gridCommon;
})(typeof window !== "undefined" ? window : this);
