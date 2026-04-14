$(document).ready(function () {
  $('[data-toggle="tooltip"]').tooltip();
});

// Json response messages functions --------------------------------------------------------------

// Function to show messages as notifications
function showNotification(message, type = "success") {
  const container = document.getElementById("notification-container");
  const notification = document.createElement("div");
  notification.className = `notification ${type}`;

  // Add dismiss button
  notification.innerHTML = `
        <span>${message}</span>
        <button type="button" class="notification-dismiss" aria-label="Close" 
                style="background: none; border: none; margin-left: 10px; cursor: pointer;">✕</button>
    `;

  container.appendChild(notification);

  // Dismiss functionality
  notification
    .querySelector(".notification-dismiss")
    .addEventListener("click", () => {
      notification.remove();
    });

  // Auto-dismiss after 5 seconds
  setTimeout(() => {
    if (notification.parentNode) {
      notification.remove();
    }
  }, 5000);
}
// ----------------------------------------Json response messagesEnd --------------------------------------------------------------

// --------------Validating image sizee

function validateImageSize(input) {
  const maxSize = 1024 * 1024; // 1MB
  const errorElement = document.getElementById("image-error");
  const allowedExtensions = ["jpg", "jpeg", "png", "gif"];

  if (input.files[0]) {
    const file = input.files[0];
    const fileExtension = file.name.split(".").pop().toLowerCase();

    // Check file type
    if (!allowedExtensions.includes(fileExtension)) {
      errorElement.textContent =
        "Invalid file type. Please upload PNG, JPG, JPEG, or GIF images only.";
      errorElement.style.display = "block";
      input.value = "";
      document.getElementById("image_preview").style.display = "none";
      return;
    }

    // Check file size
    if (file.size > maxSize) {
      errorElement.textContent = "Image size exceeds 1MB";
      errorElement.style.display = "block";
      input.value = "";
      document.getElementById("image_preview").style.display = "none";
    } else {
      errorElement.style.display = "none";
    }
  }
}

// ------------------------------------Loading icon -----------------------------------------------------------------
// Show loading spinner
function showLoading() {
  document.getElementById("loading-spinner").style.display = "block";
}

// Hide loading spinner
function hideLoading() {
  document.getElementById("loading-spinner").style.display = "none";
}

// ------------------------------------Loading icon end -----------------------------------------------------------------

function showAlert(message, type) {
  // Create a new div element for the alert
  var alert = document.createElement("div");
  alert.classList.add("alert", "alert-" + type); // Adding the appropriate classes based on alert type (success, danger, etc.)
  alert.setAttribute("role", "alert");
  alert.textContent = message;

  // Append the alert to the alerts container
  document.querySelector(".alerts-container").appendChild(alert);

  // Set a timeout to remove the alert after 3 seconds (3000 milliseconds)
  setTimeout(function () {
    alert.style.opacity = "0"; // Add a fade-out effect
    setTimeout(function () {
      alert.remove(); // Remove the alert element after fade-out
    }, 500); // Wait for fade-out to complete before removal
  }, 3000); // Alert will disappear after 3 seconds
}

document.addEventListener("DOMContentLoaded", function () {
  // Handle input, textarea, and select fields
  const fields = document.querySelectorAll(
    ".form-group input, .form-group textarea, .form-group select",
  );

  fields.forEach((field) => {
    // Check if the field has a value on page load (auto-filled or pre-filled)
    if (field.value.trim()) {
      field.closest(".form-group").classList.add("has-value");
    }

    // Add event listeners for focus, blur, and change
    field.addEventListener("focus", function () {
      this.closest(".form-group").classList.add("has-value");
    });

    field.addEventListener("blur", function () {
      if (!this.value.trim()) {
        this.closest(".form-group").classList.remove("has-value");
      }
    });

    field.addEventListener("change", function () {
      if (this.value.trim()) {
        this.closest(".form-group").classList.add("has-value");
      } else {
        this.closest(".form-group").classList.remove("has-value");
      }
    });
  });
});

// Moving Between paginated tabs---------------------------------------
function openTab(evt, tabName) {
  var i, tabcontent, tablinks;
  tabcontent = document.getElementsByClassName("tabcontent");
  for (i = 0; i < tabcontent.length; i++) {
    tabcontent[i].style.display = "none";
  }
  tablinks = document.getElementsByClassName("tablinks");
  for (i = 0; i < tablinks.length; i++) {
    tablinks[i].className = tablinks[i].className.replace(" active", "");
  }
  document.getElementById(tabName).style.display = "block";
  evt.currentTarget.className += " active";
}

document.addEventListener("DOMContentLoaded", function () {
  let defaultTab = document.querySelector(".tablinks");
  if (defaultTab) {
    defaultTab.click(); // Click the first available tab
  }
});

// Loading bar

// Show loading bar on form submission
function showLoadingBar() {
  var loadingBar = document.getElementById("loadingBar");
  loadingBar.style.display = "block"; // Show the loading bar
}

// Hide loading bar after the form is successfully submitted or when there is an error
function hideLoadingBar() {
  var loadingBar = document.getElementById("loadingBar");
  loadingBar.style.display = "none"; // Hide the loading bar
}

// Submit form with loading bar visibility
// document.querySelector("form").onsubmit = function (e) {
//   // Prevent default form submission
//   e.preventDefault();

//   // Show loading bar
//   showLoadingBar();

//   // Simulate a delay for the form submission process (e.g., an AJAX request)
//   setTimeout(function () {
//     // Hide the loading bar after the form processing is complete (e.g., on success or failure)
//     hideLoadingBar();

//     // If the form is valid, submit the form, otherwise show an error
//     if (validateForm()) {
//       e.target.submit(); // Submit the form
//     } else {
//       // Handle validation errors or form submission failure
//       alert("There was an error with the form submission.");
//     }
//   }, 5000); // Simulate a delay of 3 seconds for the form submission process (change as needed)
// };

// Add thousand separators when the input is focused or on change
// Add thousand separators to any input field with class "format-number"
document.querySelectorAll(".format-number").forEach(function (input) {
  input.addEventListener("input", function (e) {
    let value = e.target.value.replace(/,/g, ""); // Remove existing commas
    e.target.value = value.replace(/\B(?=(\d{3})+(?!\d))/g, ","); // Add thousand separators
  });
});

function formatAmount(input) {
  let value;

  // Check if input is a string/number or a DOM element
  if (typeof input === "string" || typeof input === "number") {
    // If input is a value (string or number)
    value = input.toString().replace(/,/g, "");
  } else if (input && typeof input === "object" && input.value !== undefined) {
    // If input is a DOM element with value property
    value = input.value.replace(/,/g, "");
  } else {
    return "";
  }

  if (!isNaN(value) && value !== "") {
    const formattedValue = parseFloat(value).toLocaleString("en-US");

    // If input is a DOM element, update its value
    if (input && typeof input === "object" && input.value !== undefined) {
      input.value = formattedValue;
    }

    // Return the formatted value for use in other contexts
    return formattedValue;
  }

  return value;
}

function formatCurrency(amount, currency = "USD", includeSymbol = true) {
  const value = parseFloat(amount || 0);
  const options = {
    style: includeSymbol ? "currency" : "decimal",
    currency: currency,
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
    currencyDisplay: "symbol",
  };

  try {
    return new Intl.NumberFormat(navigator.language, options).format(value);
  } catch (e) {
    // Fallback if currency is not supported
    return `${currency} ${value.toFixed(2)}`;
  }
}

document.addEventListener("DOMContentLoaded", function () {
  const filterBtn = document.getElementById("mobileFilterBtn");
  const filterWrapper = document.querySelector(".filter-wrapper");

  if (filterBtn && filterWrapper) {
    // Initialize based on screen size
    const updateFilterVisibility = () => {
      if (window.innerWidth <= 768) {
        filterWrapper.classList.remove("expanded");
      } else {
        filterWrapper.classList.add("expanded");
      }
    };

    // Set initial state
    updateFilterVisibility();

    // Toggle filters on button click
    filterBtn.addEventListener("click", () => {
      filterWrapper.classList.toggle("expanded");

      // Animate the filter icon
      filterBtn.classList.toggle("active");
    });

    // Update on window resize
    window.addEventListener("resize", updateFilterVisibility);
  }
});

function setLoadingState(isLoading) {
  let indicator = document.getElementById("loadingIndicator");
  const filterControls = document.querySelectorAll(
    ".filter-controls select, .filter-controls button",
  );

  if (isLoading) {
    if (!indicator) {
      // Create container
      indicator = document.createElement("div");
      indicator.id = "loadingIndicator";

      // Spinner element
      const spinner = document.createElement("span");
      spinner.className = "spinner";
      spinner.style.marginRight = "8px";

      // Text element with animated dots
      const text = document.createElement("span");
      text.innerHTML = `Loading<span class="dots"><span>.</span><span>.</span><span>.</span></span>`;

      indicator.appendChild(spinner);
      indicator.appendChild(text);

      Object.assign(indicator.style, {
        position: "fixed",
        top: "10px",
        right: "10px",
        padding: "8px 14px",
        backgroundColor: "#00b5b8",
        color: "white",
        borderRadius: "6px",
        zIndex: "1000",
        fontSize: "14px",
        boxShadow: "0 2px 8px rgba(0,0,0,0.2)",
        display: "flex",
        alignItems: "center",
        transform: "translateX(150%)",
        opacity: "0",
        transition: "transform 0.4s ease, opacity 0.4s ease",
      });

      // Style and animation for spinner and dots
      const style = document.createElement("style");
      style.textContent = `
                .spinner {
                    width: 14px;
                    height: 14px;
                    border: 2px solid white;
                    border-top: 2px solid rgba(255, 255, 255, 0.3);
                    border-radius: 50%;
                    animation: spin 0.8s linear infinite;
                }
                @keyframes spin {
                    0% { transform: rotate(0deg); }
                    100% { transform: rotate(360deg); }
                }

                .dots span {
                    animation: blink 1.4s infinite;
                    opacity: 0;
                }
                .dots span:nth-child(1) { animation-delay: 0s; }
                .dots span:nth-child(2) { animation-delay: 0.2s; }
                .dots span:nth-child(3) { animation-delay: 0.4s; }

                @keyframes blink {
                    0%, 80%, 100% { opacity: 0; }
                    40% { opacity: 1; }
                }
            `;
      document.head.appendChild(style);

      document.body.appendChild(indicator);
      void indicator.offsetWidth; // Trigger reflow
    }

    // Show it
    indicator.style.transform = "translateX(0)";
    indicator.style.opacity = "1";

    filterControls.forEach((control) => (control.disabled = true));
  } else {
    if (indicator) {
      indicator.style.transform = "translateX(150%)";
      indicator.style.opacity = "0";
      indicator.addEventListener(
        "transitionend",
        () => {
          if (indicator.parentNode) indicator.remove();
        },
        { once: true },
      );
    }

    filterControls.forEach((control) => (control.disabled = false));
  }
}

// function formatCurrency(amount, currency = baseCurrency, includeSymbol = true) {
//   const value = parseFloat(amount || 0);
//   const options = {
//     style: includeSymbol ? "currency" : "decimal",
//     currency: currency,
//     minimumFractionDigits: 2,
//     maximumFractionDigits: 2,
//   };

//   try {
//     return new Intl.NumberFormat(navigator.language, options).format(value);
//   } catch (e) {
//     // Fallback if currency is not supported
//     return `${currency} ${value.toFixed(2)}`;
//   }
// }

function updateDoNotShowAgainPreference(preferenceType, doNotShowAgain) {
  fetch("/update_do_not_show_again", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-CSRFToken": csrfToken,
    },
    body: JSON.stringify({
      preference_type: preferenceType,
      do_not_show_again: doNotShowAgain,
    }),
  })
    .then((response) => response.json())
    .then((data) => {
      if (data.success) {
        showNotification(`Preference updated successfully.`, "success");
      } else {
        console.error(
          `Failed to update preference "${preferenceType}":`,
          data.error,
        );
        showNotification(`Failed to update preference: ${data.error}`, "error");
      }
    })
    .catch((error) => {
      console.error(`Error updating preference "${preferenceType}":`, error);
      showNotification(`Error updating preference: ${error}`, "error");
    });
}

document.addEventListener("DOMContentLoaded", () => {
  document
    .querySelectorAll("input.do-not-show-again-checkbox")
    .forEach((checkbox) => {
      checkbox.addEventListener("change", () => {
        const preferenceType = checkbox.dataset.preferenceType;
        const doNotShowAgain = checkbox.checked;

        if (!preferenceType) {
          console.error(
            "Missing data-preference-type attribute on Do Not Show Again checkbox",
          );
          return;
        }

        updateDoNotShowAgainPreference(preferenceType, doNotShowAgain);
      });
    });
});

// Add this at the top of your script
function debounce(func, wait) {
  let timeout;
  return function (...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(this, args), wait);
  };
}

function setDefaultDateRange() {
  const today = new Date();
  const firstDayOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);

  // Format dates as YYYY-MM-DD
  const formatDate = (date) => date.toISOString().split("T")[0];

  document.getElementById("start-date").value = formatDate(firstDayOfMonth);
  document.getElementById("end-date").value = formatDate(today);
}

// Replace all parseFloat() calls with a safer version
function safeParseNumber(value) {
  if (value === null || value === undefined) return 0;
  const num = parseFloat(value);
  return isNaN(num) ? 0 : num;
}

document.addEventListener("DOMContentLoaded", () => {
  const sidebar = document.getElementById("mobileShortcutsSidebar");
  const openBtn = document.getElementById("shortcutToggleBtn");
  const closeBtn = document.getElementById("closeShortcutsBtn");

  if (!sidebar || !openBtn || !closeBtn) return; // safety check

  openBtn.addEventListener("click", () => {
    sidebar.classList.add("active");
  });

  closeBtn.addEventListener("click", () => {
    sidebar.classList.remove("active");
  });

  // Optional: Close sidebar if clicked outside
  window.addEventListener("click", (e) => {
    if (
      sidebar.classList.contains("active") &&
      !sidebar.contains(e.target) &&
      e.target !== openBtn
    ) {
      sidebar.classList.remove("active");
    }
  });

  // Only show button on mobile
  function checkMobile() {
    if (window.innerWidth <= 768) {
      openBtn.style.display = "inline-block";
    } else {
      openBtn.style.display = "none";
      sidebar.classList.remove("active");
    }
  }
  checkMobile();
  window.addEventListener("resize", checkMobile);
});

function showNotificationWithDetails(message, type, failedTransactions) {
  if (failedTransactions.length === 0) {
    showNotification(message, type);
    return;
  }

  const failedList = failedTransactions
    .map((tx) => `<li><strong>Deduction ${tx.id}:</strong> ${tx.reason}</li>`)
    .join("");

  Swal.fire({
    title: type === "warning" ? "Completed with Errors" : "Error",
    html: `
            <p>${message}</p>
            <div style="text-align: left; margin-top: 15px;">
                <h4 style="margin-bottom: 10px; color: #dc3545;">Failed Deductions:</h4>
                <ul style="padding-left: 20px; max-height: 200px; overflow-y: auto;">
                    ${failedList}
                </ul>
            </div>
        `,
    icon: type === "warning" ? "warning" : "error",
    showConfirmButton: true,
    confirmButtonText: "OK",
    showCloseButton: true,
    allowOutsideClick: false,
    allowEscapeKey: false,
    width: "600px",
    customClass: {
      popup: "custom-swal-popup",
      title: "custom-swal-title",
      content: "custom-swal-content",
    },
  });
}

function formatDate(dateString) {
  if (!dateString) return "";
  const date = new Date(dateString);
  return date.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}
function formatNumber(number) {
  return number.toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  });
}
