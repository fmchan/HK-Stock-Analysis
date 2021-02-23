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
