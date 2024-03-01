// wait for the DOM to finish loading
document.addEventListener('DOMContentLoaded', function () {
    // bind the form to the function
    // load in html template for the control panel
    $(".custom_legend").load("static/html/legend.html");
    // disable propagation to prevent map click event from firing
    // $(".custom_legend").mouseover(function (f) {
    //     $("#map").css("pointer-events", "none");
    // });
    // $(".custom_legend").mouseout(function (f) {
    //     $("#map").css("pointer-events", "auto");
    // });
    // toggle the legend
    $(".custom_legend").click(function (f) {
        // stop the map click event from firing        
        if (f.target.classList.contains("legend_icon")) {
            $("#" + f.target.id).toggleClass("turned_off");
        }
        if (f.target.id === "legend_selected_wb_layer_icon") {
            $(".selected-wb-layer").toggle(200);
        }
        if (f.target.id === "legend_upstream_layer_icon") {
            $(".upstream-wb-layer").toggle(200);
        }

        // disable propagation to prevent map click event from firing
        f.stopPropagation();
        return;

    });

});


async function toggle_selected_catchments() {
}