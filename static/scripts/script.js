$(document).on('click', '.bookmark', function(ev) {
  var sid = $(ev.currentTarget).attr('sid');
  var pattern = $(ev.currentTarget).attr('pattern');
  $(this).val("bookmarked").attr("disabled", "true");
  $.ajax({
    type: "POST",
    url: "bookmark",
    data: JSON.stringify({'sid':sid, 'pattern':pattern}),
    contentType: 'application/json;charset=UTF-8',
    success: function (data) {
        console.log(data);
        alert(data);
      }
  });
});

$(document).on('click', '.archive', function(ev) {
  var sid = $(ev.currentTarget).attr('sid');
  var pattern = $(ev.currentTarget).attr('pattern');
  $.ajax({
    type: "POST",
    url: "archive",
    data: JSON.stringify({'sid':sid, 'pattern':pattern}),
    contentType: 'application/json;charset=UTF-8',
    success: function (data) {
        console.log(data);
        alert(data);
    },
    complete: function() {
      location.reload();
    }
  });
});

$(document).on('click', '.delete', function(ev) {
  var sid = $(ev.currentTarget).attr('sid');
  var pattern = $(ev.currentTarget).attr('pattern');
  var start_date = $(ev.currentTarget).attr('start_date');
  $.ajax({
    type: "POST",
    url: "delete",
    data: JSON.stringify({'sid':sid, 'pattern':pattern, 'start_date':start_date}),
    contentType: 'application/json;charset=UTF-8',
    success: function (data) {
        console.log(data);
        alert(data);
    },
    complete: function() {
      location.reload();
    }
  });
});

document.addEventListener('DOMContentLoaded', function() {
  var price_movement_cells = document.getElementsByClassName("price_movement");
  for(var i=0; i<price_movement_cells.length; i++) {
    price_movement_cell = price_movement_cells[i]
    if (parseFloat(price_movement_cell.innerHTML, 10) < 0) {
      price_movement_cell.insertAdjacentHTML('afterbegin', '⇩ ');
      price_movement_cell.insertAdjacentHTML('beforeend', ' %');
      price_movement_cell.style.color = "red";
    }
    else if (parseFloat(price_movement_cell.innerHTML, 10) > 0) {
      price_movement_cell.insertAdjacentHTML('afterbegin', '⇧ ');
      price_movement_cell.insertAdjacentHTML('beforeend', ' %');
      price_movement_cell.style.color = "green";
    }
  }
}, false);