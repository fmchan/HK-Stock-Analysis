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

function sortList(primitive, field) {
  var list, i, switching, b, shouldSwitch;
  list = document.getElementById("sideul");
  switching = true;
  sort = document.getElementsByClassName(field + '_btn sort_btn')[0].getAttribute("sort")
  while (switching) {
    switching = false;
    b = list.getElementsByTagName("li");
    for (i = 0; i < (b.length - 1); i++) {
      shouldSwitch = false;
      if (primitive == "date") {
        if (sort == "asc") {
          if (new Date(b[i].getAttribute(field)) - new Date(b[i + 1].getAttribute(field)) > 0) {
            shouldSwitch = true;
            break;
          }
        }
        else if (sort == "desc") {
          if (new Date(b[i].getAttribute(field)) - new Date(b[i + 1].getAttribute(field)) < 0) {
            shouldSwitch = true;
            break;
          }
        }
      }
      else if (primitive == "float") {
        if (sort == "asc") {
          if ((Math.round(b[i].getAttribute(field) * 100) > Math.round(b[i + 1].getAttribute(field) * 100))) {
            shouldSwitch = true;
            break;
          }
        }
        else if (sort == "desc") {
          if ((Math.round(b[i].getAttribute(field) * 100) < Math.round(b[i + 1].getAttribute(field) * 100))) {
            shouldSwitch = true;
            break;
          }
        }
      }
    }
    if (shouldSwitch) {
      b[i].parentNode.insertBefore(b[i + 1], b[i]);
      switching = true;
    }
  }
  if (sort == "asc") {
    document.getElementsByClassName(field + '_btn sort_btn')[0].setAttribute('sort', 'desc');
    document.getElementsByClassName(field + '_btn sort_btn')[0].textContent = field + ' ↓';
  }
  else {
    document.getElementsByClassName(field + '_btn sort_btn')[0].setAttribute('sort', 'asc');
    document.getElementsByClassName(field + '_btn sort_btn')[0].textContent = field + ' ↑';
  }
  
}

// function sortList(primitive, field, sort) {
//   var list, i, switching, b, shouldSwitch;
//   list = document.getElementById("sideul");
//   switching = true;
//   while (switching) {
//     switching = false;
//     b = list.getElementsByTagName("li");
//     for (i = 0; i < (b.length - 1); i++) {
//       shouldSwitch = false;
//       if (primitive == "date") {
//         if (sort == "asc") {
//           if (new Date(b[i].getAttribute(field)) - new Date(b[i + 1].getAttribute(field)) > 0) {
//             shouldSwitch = true;
//             break;
//           }
//         }
//         else if (sort == "desc") {
//           if (new Date(b[i].getAttribute(field)) - new Date(b[i + 1].getAttribute(field)) < 0) {
//             shouldSwitch = true;
//             break;
//           }
//         }
//       }
//       else if (primitive == "float") {
//         if (sort == "asc") {
//           if ((Math.round(b[i].getAttribute(field) * 100) > Math.round(b[i + 1].getAttribute(field) * 100))) {
//             shouldSwitch = true;
//             break;
//           }
//         }
//         else if (sort == "desc") {
//           if ((Math.round(b[i].getAttribute(field) * 100) < Math.round(b[i + 1].getAttribute(field) * 100))) {
//             shouldSwitch = true;
//             break;
//           }
//         }
//       }
//     }
//     if (shouldSwitch) {
//       b[i].parentNode.insertBefore(b[i + 1], b[i]);
//       switching = true;
//     }
//   }
// }