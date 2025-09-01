fetch("/data")
  .then(res => res.json())
  .then(data => {
    let table = document.getElementById("ranking");
    data.forEach(row => {
      let tr = document.createElement("tr");
      row.forEach(cell => {
        let td = document.createElement("td");
        td.innerText = cell;
        tr.appendChild(td);
      });
      table.appendChild(tr);
    });
  });
