// Helper functions for line items
function showInventoryFields(lineItem) {
  const itemField = lineItem.querySelector(".item-field");
  const itemDescriptionField = lineItem.querySelector(
    ".item-description-field",
  );
  const inventoryDropdown = lineItem.querySelector(".inventory-dropdown");
  const warehouseGroup = lineItem.querySelector(".warehouse-group");
  const warehouseDropdown = lineItem.querySelector(".warehouse-dropdown");
  const uomReadonly = lineItem.querySelector(".uom-readonly");
  const uomHidden = lineItem.querySelector(".uom-hidden");
  const uomDropdown = lineItem.querySelector(".uom-dropdown");

  // Disable regular item field
  itemField.disabled = true;
  itemField.classList.add("disabled-field");
  itemField.value = "";

  // Handle description field
  itemDescriptionField.disabled = true;
  itemDescriptionField.classList.add("disabled-field");
  itemDescriptionField.value = "";
  lineItem.querySelector(".item-description-group").style.display = "none";

  // Show inventory-specific fields
  warehouseGroup.style.display = "block";
  inventoryDropdown.style.display = "block";
  lineItem.querySelector('.form-group input[name="item[]"]').style.display =
    "none";

  // Show readonly UOM field and hide dropdown
  uomReadonly.style.display = "block";
  uomDropdown.style.display = "none";

  // REMOVE REQUIRED ATTRIBUTE FOR INVENTORY ITEMS (UOM is auto-populated)
  uomDropdown.removeAttribute("required");
  uomHidden.removeAttribute("required");

  // Initialize Select2 with event listeners
  // Initialize Select2 with search enabled and auto-focus
  setTimeout(() => {
    $(inventoryDropdown)
      .select2({
        width: "100%",
        placeholder: "Search and select inventory item...",
        allowClear: true,
        minimumResultsForSearch: 1, // Always show search box
      })
      .on("select2:open", function () {
        // Auto-focus the search input when dropdown opens
        setTimeout(function () {
          document
            .querySelector(".select2-container--open .select2-search__field")
            .focus();
        }, 100);
      })
      .on("change", function () {
        updateAvailableQuantity(lineItem);
        updateUOMForInventoryItem(lineItem);
      });

    $(warehouseDropdown)
      .select2({
        width: "100%",
        placeholder: "Search and select warehouse...",
        allowClear: true,
        minimumResultsForSearch: 1, // Always show search box
      })
      .on("select2:open", function () {
        // Auto-focus the search input when dropdown opens
        setTimeout(function () {
          document
            .querySelector(".select2-container--open .select2-search__field")
            .focus();
        }, 100);
      })
      .on("change", function () {
        updateAvailableQuantity(lineItem);
      });
  }, 50);

  // Check if we already have values and update immediately
  if (inventoryDropdown.value) {
    updateUOMForInventoryItem(lineItem);
  }
  if (inventoryDropdown.value && warehouseDropdown.value) {
    updateAvailableQuantity(lineItem);
  }
}

function showRegularFields(lineItem) {
  const itemField = lineItem.querySelector(".item-field");
  const itemDescriptionField = lineItem.querySelector(
    ".item-description-field",
  );
  const inventoryDropdown = lineItem.querySelector(".inventory-dropdown");
  const warehouseGroup = lineItem.querySelector(".warehouse-group");
  const uomReadonly = lineItem.querySelector(".uom-readonly");
  const uomHidden = lineItem.querySelector(".uom-hidden");
  const uomDropdown = lineItem.querySelector(".uom-dropdown");

  console.log("Show rgular fields has been called");

  // Hide inventory-specific fields
  inventoryDropdown.style.display = "none";
  warehouseGroup.style.display = "none";

  // Destroy Select2 instances
  $(inventoryDropdown).select2("destroy");
  $(warehouseGroup.querySelector(".warehouse-dropdown")).select2("destroy");

  // Enable regular fields
  itemField.disabled = false;
  itemField.classList.remove("disabled-field");
  itemField.value = "";

  // Handle description field
  itemDescriptionField.disabled = false;
  itemDescriptionField.classList.remove("disabled-field");
  itemDescriptionField.value = "";
  lineItem.querySelector(".item-description-group").style.display = "block";

  // Show regular item input
  lineItem.querySelector('.form-group input[name="item[]"]').style.display =
    "block";

  // Show UOM dropdown and hide readonly field
  uomReadonly.style.display = "none";
  uomDropdown.style.display = "block";

  // MAKE UOM REQUIRED FOR NON-INVENTORY ITEMS
  uomDropdown.setAttribute("required", "required");

  uomHidden.removeAttribute("required"); // Remove required from hidden field
}

// Function to update UOM field when inventory item is selected
function updateUOMForInventoryItem(lineItem) {
  const inventoryDropdown = lineItem.querySelector(".inventory-dropdown");
  const uomReadonly = lineItem.querySelector(".uom-readonly");
  const uomHidden = lineItem.querySelector(".uom-hidden");
  const uomDropdown = lineItem.querySelector(".uom-dropdown");

  // Check if inventory dropdown has a selected option
  if (inventoryDropdown.selectedIndex === -1) {
    uomReadonly.value = "";
    uomHidden.value = "";
    uomReadonly.placeholder = "Select an inventory item first";
    return;
  }

  const selectedOption =
    inventoryDropdown.options[inventoryDropdown.selectedIndex];
  const uomId = selectedOption.getAttribute("data-uom-id");
  const uomName = selectedOption.getAttribute("data-uom-name");

  console.log("Updating UOM:", {
    uomId,
    uomName,
    selectedOption: selectedOption.text,
  });

  if (uomId && uomName) {
    uomReadonly.value = uomName;
    uomHidden.value = uomId;
    uomDropdown.value = uomId;
    uomReadonly.placeholder = "UOM from inventory item";
  } else {
    uomReadonly.value = "";
    uomHidden.value = "";
    uomDropdown.value = "";
    uomReadonly.placeholder = "No UOM assigned to this item";
  }
}
// Add this if you need to access these functions from other scripts
window.lineItemFunctions = {
  showInventoryFields,
  showRegularFields,
};

// Function to calculate line item total (including discounts and taxes)
function calculateLineItemTotal(lineItem) {
  const quantity =
    parseFloat(lineItem.querySelector('input[name="quantity[]"]').value) || 0;
  const unitPrice =
    parseFloat(lineItem.querySelector('input[name="unit_price[]"]').value) || 0;

  // Calculate subtotal (before applying discounts or taxes)
  let subtotal = quantity * unitPrice;

  // Apply discounts (if any)
  const discountFields = lineItem.querySelectorAll(
    '.discount-tax-fields select[name="discount_type[]"]',
  );
  const discountValues = lineItem.querySelectorAll(
    '.discount-tax-fields input[name="discount_value[]"]',
  );

  discountFields.forEach((discountField, index) => {
    const discountType = discountField.value;
    const discountValue = parseFloat(discountValues[index].value) || 0;

    if (discountType === "amount") {
      subtotal -= discountValue; // Subtract fixed amount
    } else if (discountType === "percentage") {
      subtotal -= (subtotal * discountValue) / 100; // Subtract percentage
    }
  });

  // Apply taxes (if any) to the subtotal (after discounts)
  const taxFields = lineItem.querySelectorAll(
    '.discount-tax-fields input[name="tax[]"]',
  );
  taxFields.forEach((taxField) => {
    const tax = parseFloat(taxField.value) || 0;
    subtotal += (subtotal * tax) / 100; // Add tax percentage
  });

  // Round the subtotal to two decimal places before returning it
  return subtotal.toFixed(2); // Return the total with 2 decimal places
}

// Function to update the line item total display
function updateLineItemTotal(lineItem) {
  const lineItemTotal = calculateLineItemTotal(lineItem);

  // Format with thousand separators only (no currency symbol)
  const formattedTotal = formatCurrency(lineItemTotal, "USD", false);

  // Find element with class OR ID
  const totalSpan =
    lineItem.querySelector(".line-item-total-amount") ||
    lineItem.querySelector("#line-item-total-amount");

  if (totalSpan) {
    totalSpan.textContent = formattedTotal;
  }
}
// // Function to calculate general total, total discount, and total tax
// function calculateTotals() {
//   const lineItems = document.querySelectorAll(".line-item");

//   let subtotal = 0;
//   let totalDiscount = 0;
//   let totalTax = 0;
//   let totalLineCost = 0;

//   lineItems.forEach((lineItem) => {
//     const quantity =
//       parseFloat(lineItem.querySelector('input[name="quantity[]"]').value) || 0;
//     const unitPrice =
//       parseFloat(lineItem.querySelector('input[name="unit_price[]"]').value) ||
//       0;

//     // Calculate line subtotal
//     let lineSubtotal = quantity * unitPrice;

//     // Calculate discounts for this line item
//     let lineDiscount = 0;
//     const discountFields = lineItem.querySelectorAll(
//       '.discount-tax-fields select[name="discount_type[]"]'
//     );
//     const discountValues = lineItem.querySelectorAll(
//       '.discount-tax-fields input[name="discount_value[]"]'
//     );

//     discountFields.forEach((discountField, index) => {
//       const discountType = discountField.value;
//       const discountValue = parseFloat(discountValues[index].value) || 0;

//       if (discountType === "amount") {
//         lineDiscount += discountValue; // Add fixed amount discount
//       } else if (discountType === "percentage") {
//         lineDiscount += (lineSubtotal * discountValue) / 100; // Add percentage discount
//       }
//     });

//     // Calculate taxes for this line item after discount
//     let lineTax = 0;
//     const taxFields = lineItem.querySelectorAll(
//       '.discount-tax-fields input[name="tax[]"]'
//     );
//     taxFields.forEach((taxField) => {
//       const tax = parseFloat(taxField.value) || 0;
//       lineTax += ((lineSubtotal - lineDiscount) * tax) / 100; // Apply tax after discount
//     });

//     // Calculate final cost per line item
//     let lineTotalCost = lineSubtotal - lineDiscount + lineTax;

//     // Update running totals
//     subtotal += lineSubtotal;
//     totalDiscount += lineDiscount;
//     totalTax += lineTax;
//     totalLineCost += lineTotalCost; // Total of all line items
//   });

//   // Apply overall discount
//   const overallDiscountType = document.getElementById(
//     "overall_discount_type"
//   ).value;
//   const overallDiscountValue =
//     parseFloat(document.getElementById("overall_discount_value").value) || 0;
//   let overallDiscount = 0;

//   if (overallDiscountType === "amount") {
//     overallDiscount = overallDiscountValue;
//   } else if (overallDiscountType === "percentage") {
//     overallDiscount = (totalLineCost * overallDiscountValue) / 100;
//   }

//   // **Fix: Include overall discount in total discount**
//   totalDiscount += overallDiscount;

//   // Apply overall tax (after discount)
//   const overallTax =
//     parseFloat(document.getElementById("overall_tax").value) || 0;
//   let additionalOverallTax =
//     (totalLineCost - overallDiscount) * (overallTax / 100);

//   // Get shipping and handling costs
//   const shippingCost =
//     parseFloat(document.getElementById("shipping_cost").value) || 0;
//   const handlingCost =
//     parseFloat(document.getElementById("handling_cost").value) || 0;

//   // Final general total, including shipping and handling
//   const generalTotal =
//     totalLineCost -
//     overallDiscount +
//     additionalOverallTax +
//     shippingCost +
//     handlingCost;

//   return {
//     subtotal: subtotal.toFixed(2),
//     totalLineCost: totalLineCost.toFixed(2),
//     totalDiscount: totalDiscount.toFixed(2),
//     totalTax: (totalTax + additionalOverallTax).toFixed(2),
//     shippingCost: shippingCost.toFixed(2),
//     handlingCost: handlingCost.toFixed(2),
//     generalTotal: generalTotal.toFixed(2),
//   };
// }

function calculateTotals() {
  const lineItems = document.querySelectorAll(".line-item");

  let subtotal = 0;
  let totalDiscount = 0;
  let totalTax = 0;
  let totalLineCost = 0;

  // New totals for specific categories
  let netInventoryTotal = 0; // Sum of inventory items (before tax)
  let netNonInventoryTotal = 0; // Sum of non-inventory items (before tax)
  let totalExpenses = 0; // Sum of services (before tax)
  let totalOtherTax = 0; // Sum of all taxes (including shipping/handling)

  lineItems.forEach((lineItem) => {
    const quantity =
      parseFloat(lineItem.querySelector('input[name="quantity[]"]').value) || 0;
    const unitPrice =
      parseFloat(lineItem.querySelector('input[name="unit_price[]"]').value) ||
      0;
    const itemType = lineItem.querySelector(".item-type-field").value;

    // Line subtotal
    let lineSubtotal = quantity * unitPrice;

    // Discounts
    let lineDiscount = 0;
    const discountFields = lineItem.querySelectorAll(
      '.discount-tax-fields select[name="discount_type[]"]',
    );
    const discountValues = lineItem.querySelectorAll(
      '.discount-tax-fields input[name="discount_value[]"]',
    );
    discountFields.forEach((discountField, index) => {
      const discountType = discountField.value;
      const discountValue = parseFloat(discountValues[index].value) || 0;
      if (discountType === "amount") lineDiscount += discountValue;
      else if (discountType === "percentage")
        lineDiscount += (lineSubtotal * discountValue) / 100;
    });

    // Taxes after discount
    let lineTax = 0;
    const taxFields = lineItem.querySelectorAll(
      '.discount-tax-fields input[name="tax[]"]',
    );
    taxFields.forEach((taxField) => {
      const tax = parseFloat(taxField.value) || 0;
      lineTax += ((lineSubtotal - lineDiscount) * tax) / 100;
    });

    const netLineTotal = lineSubtotal - lineDiscount;

    // Categorize by item type
    if (itemType === "inventory") netInventoryTotal += netLineTotal;
    else if (itemType === "service") totalExpenses += netLineTotal;
    else netNonInventoryTotal += netLineTotal;

    totalOtherTax += lineTax;

    const lineTotalCost = netLineTotal + lineTax;

    subtotal += lineSubtotal;
    totalDiscount += lineDiscount;
    totalTax += lineTax;
    totalLineCost += lineTotalCost;
  });

  // Overall discount
  const overallDiscountType = document.getElementById(
    "overall_discount_type",
  ).value;
  const overallDiscountValue =
    parseFloat(document.getElementById("overall_discount_value").value) || 0;
  let overallDiscount = 0;
  if (overallDiscountType === "amount") overallDiscount = overallDiscountValue;
  else if (overallDiscountType === "percentage")
    overallDiscount = (totalLineCost * overallDiscountValue) / 100;
  totalDiscount += overallDiscount;

  // Overall tax
  const overallTax =
    parseFloat(document.getElementById("overall_tax").value) || 0;
  let additionalOverallTax =
    (totalLineCost - overallDiscount) * (overallTax / 100);

  // Shipping & handling
  const shippingCost =
    parseFloat(document.getElementById("shipping_cost").value) || 0;
  const handlingCost =
    parseFloat(document.getElementById("handling_cost").value) || 0;

  totalOtherTax += additionalOverallTax + shippingCost + handlingCost;
  totalExpenses += shippingCost + handlingCost;

  const generalTotal =
    totalLineCost -
    overallDiscount +
    additionalOverallTax +
    shippingCost +
    handlingCost;

  return {
    subtotal: subtotal.toFixed(2),
    totalLineCost: totalLineCost.toFixed(2),
    totalDiscount: totalDiscount.toFixed(2),
    totalTax: (totalTax + additionalOverallTax).toFixed(2),
    shippingCost: shippingCost.toFixed(2),
    handlingCost: handlingCost.toFixed(2),
    generalTotal: generalTotal.toFixed(2),
    netInventoryTotal: netInventoryTotal.toFixed(2),
    netNonInventoryTotal: netNonInventoryTotal.toFixed(2),
    totalExpenses: totalExpenses.toFixed(2),
    totalOtherTax: totalOtherTax.toFixed(2),
  };
}

// Function to update summary section
function updateSummary() {
  try {
    const {
      totalLineCost,
      totalDiscount,
      totalTax,
      shippingCost,
      handlingCost,
      generalTotal,
    } = calculateTotals();

    // Safely update totals in the summary
    safeUpdateText(
      "#totalItems",
      document.querySelectorAll(".line-item").length,
    );
    safeUpdateText(
      "#totalLineSubtotals",
      formatCurrency(totalLineCost, "USD", false),
    );
    safeUpdateText(
      "#totalDiscount",
      formatCurrency(totalDiscount, "USD", false),
    );
    safeUpdateText("#totalTax", formatCurrency(totalTax, "USD", false));
    safeUpdateText("#shippingCost", formatCurrency(shippingCost, "USD", false));
    safeUpdateText("#handlingCost", formatCurrency(handlingCost, "USD", false));
    safeUpdateText("#generalTotal", formatCurrency(generalTotal, "USD", false));
    safeUpdateText("#totalLineSubtotal", totalLineCost);
    // Safely update customer and currency section
    // Safely update customer/vendor and currency section
    const customerInput = document.getElementById("customer_name");
    const vendorInput = document.getElementById("vendor_name");
    const currencySelect = document.getElementById("currency");

    // Check for customer first, then vendor
    if (customerInput) {
      const customerName = customerInput.value.trim();
      safeUpdateText("#summaryCustomer", customerName || "-");
    } else if (vendorInput) {
      const vendorName = vendorInput.value.trim();
      safeUpdateText("#summaryvendor", vendorName || "-");
    }

    if (currencySelect) {
      const currencyText =
        currencySelect.options[currencySelect.selectedIndex]?.text;
      safeUpdateText("#summaryCurrency", currencyText || "-");
    }

    // Always mirror general total into Amount Paid input
    const amountPaidInput = document.getElementById("amount_paid");
    if (amountPaidInput) {
      const numericTotal = Number(generalTotal);
      amountPaidInput.value = Number.isFinite(numericTotal)
        ? numericTotal.toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
          })
        : generalTotal; // fallback if not numeric
    }

    // Safely toggle sections only if they exist
    safelyToggleSection("taxAllocationSection", toggleTaxAllocationSection);

    if (typeof toggleCreditAccounts === "function") {
      safelyToggleSection("creditAccountsSection", toggleCreditAccounts);
    }

    // ADD THIS LINE - Update converted total display
    updateConvertedTotal();
  } catch (error) {
    console.error("Error updating summary:", error);
    // Optionally show user-friendly error message
  }
}

// Function to update converted total display
function updateConvertedTotal() {
  const convertedRow = document.getElementById("convertedTotalRow");
  const convertedSpan = document.getElementById("convertedTotal");
  const convertedCurrency = document.getElementById("convertedCurrency");

  if (!convertedRow || !convertedSpan || !convertedCurrency) return;

  if (window.rateManager && window.rateManager.isForeignCurrency()) {
    const rate = window.rateManager.getRate();
    if (rate > 0) {
      // Get the general total
      const generalTotal =
        parseFloat(
          document.getElementById("generalTotal").textContent.replace(/,/g, ""),
        ) || 0;
      const convertedTotal = generalTotal * rate;
      const baseCurrencyCode = window.rateManager.getBaseCurrencyCode();

      convertedSpan.textContent = formatCurrency(
        convertedTotal,
        baseCurrencyCode,
        false,
      );
      convertedCurrency.textContent = baseCurrencyCode;
      convertedRow.style.display = "block";
      return;
    }
  }
  convertedRow.style.display = "none";
}

// Helper functions for safe DOM manipulation
function safeUpdateText(selector, value) {
  const element = document.querySelector(selector);
  if (element) {
    element.textContent = formatValue(value);
  }
}

function formatValue(value) {
  // Add any formatting logic here (e.g., for currency)
  return value ?? "-";
}

function safelyToggleSection(sectionId, toggleFunction) {
  const section = document.getElementById(sectionId);
  if (section) {
    try {
      toggleFunction();
    } catch (toggleError) {
      console.error(`Error toggling ${sectionId}:`, toggleError);
    }
  }
}

// Function to update available quantity for inventory items
// This function should be called when the inventory item or warehouse selection changes

// function updateAvailableQuantity(lineItem) {
//   // Only proceed if this is an inventory item
//   const itemType = lineItem.querySelector(".item-type-field").value;
//   if (itemType !== "inventory") {
//     lineItem.querySelector(".stock-display").innerHTML = "";
//     return;
//   }

//   const inventoryDropdown = lineItem.querySelector(".inventory-dropdown");
//   const warehouseDropdown = lineItem.querySelector(".warehouse-dropdown");
//   const stockDisplay = lineItem.querySelector(".stock-display");

//   const inventoryItemId = $(inventoryDropdown).val();

//   let warehouseId;
//   if (warehouseDropdown) {
//     warehouseId = $(warehouseDropdown).val();
//   }
//   if (!warehouseId) {
//     warehouseId = $("#location").val(); // fallback to global location dropdown
//   }

//   // Clear display if selections aren't complete
//   if (!inventoryItemId || !warehouseId) {
//     stockDisplay.innerHTML =
//       '<div class="badge bg-secondary">Please select both item and warehouse</div>';
//     return;
//   }

//   // Show loading state
//   stockDisplay.innerHTML = '<div class="stock-loading">Checking stock...</div>';

//   // Fetch available quantity from new API
//   const url = `/api/inventory_stock?item_ids[]=${inventoryItemId}&location_id=${warehouseId}&include_negative=true&use_variation_ids=true`;

//   // Fetch available quantity using the existing api_inventory_stock endpoint
//   fetch(
//     `/api/inventory_stock?item_ids[]=${inventoryItemId}&location_id=${warehouseId}&include_negative=true&use_variation_ids=true`,
//   )
//     .then((response) => {
//       if (!response.ok) throw new Error("Network response was not ok");
//       return response.json();
//     })
//     .then((data) => {
//       if (!data.success) {
//         throw new Error(data.message || "Failed to fetch stock data");
//       }

//       // Extract available quantity from the response
//       // The response structure is: {stock_data: {item_id: quantity}}
//       const availableQty = data.stock_data[inventoryItemId] || 0;
//       let badgeClass = "bg-info";
//       let warningText = "";

//       if (availableQty < 0) {
//         badgeClass = "bg-danger"; // red badge
//         warningText = " (Negative stock!)";
//         showNotification(
//           `⚠️ Warning: Stock for this item is negative (${availableQty}). Please review inventory adjustments.`,
//           "error",
//         );
//       } else if (availableQty === 0) {
//         badgeClass = "bg-warning"; // yellow badge
//         warningText = " (Out of stock)";
//       }

//       stockDisplay.innerHTML = `
//                 <div class="badge ${badgeClass}">
//                     <span>Available: ${availableQty}${warningText}</span>
//                 </div>
//             `;
//     })
//     .catch((error) => {
//       console.error("Error fetching available quantity:", error);
//       stockDisplay.innerHTML =
//         '<div class="badge bg-danger">Error loading quantity</div>';
//     });
// }

function updateAvailableQuantity(lineItem) {
  // Only proceed if this is an inventory item
  const itemType = lineItem.querySelector(".item-type-field").value;
  if (itemType !== "inventory") {
    const stockDisplay = lineItem.querySelector(".stock-display");
    if (stockDisplay) stockDisplay.innerHTML = "";
    return;
  }

  const inventoryDropdown = lineItem.querySelector(".inventory-dropdown");
  const warehouseDropdown = lineItem.querySelector(".warehouse-dropdown");
  const stockDisplay = lineItem.querySelector(".stock-display");

  const inventoryItemId = $(inventoryDropdown).val();

  let warehouseId;
  if (warehouseDropdown) {
    warehouseId = $(warehouseDropdown).val();
  }
  if (!warehouseId) {
    warehouseId = $("#location").val(); // fallback to global location dropdown
  }

  // Clear display if selections aren't complete
  if (!inventoryItemId || !warehouseId) {
    if (stockDisplay) {
      stockDisplay.innerHTML =
        '<div class="badge bg-secondary">Please select both item and warehouse</div>';
    }
    return;
  }

  // Show loading state
  if (stockDisplay) {
    stockDisplay.innerHTML =
      '<div class="stock-loading">Checking stock...</div>';
  }

  // ✅ Check if this is a purchase form by looking for unique elements
  const isPurchaseForm =
    document.querySelector(".purchase_order-main-content") !== null ||
    document.querySelector(".purchase-main-content") !== null;

  // ✅ Use different API based on context
  const apiUrl = isPurchaseForm
    ? `/api/inventory_stock_with_cost?item_ids[]=${inventoryItemId}&location_id=${warehouseId}&include_negative=true&use_variation_ids=true`
    : `/api/inventory_stock?item_ids[]=${inventoryItemId}&location_id=${warehouseId}&include_negative=true&use_variation_ids=true`;

  fetch(apiUrl)
    .then((response) => {
      if (!response.ok) throw new Error("Network response was not ok");
      return response.json();
    })
    .then((data) => {
      if (!data.success) {
        throw new Error(data.message || "Failed to fetch stock data");
      }

      const stockInfo = data.stock_data[inventoryItemId] || 0;
      const availableQtyUnformatted = isPurchaseForm
        ? stockInfo.quantity
        : stockInfo;
      const avgCost = isPurchaseForm ? stockInfo.average_cost : 0;

      // ✅ Format the available quantity using formatAmount
      const availableQty =
        typeof formatAmount === "function"
          ? formatAmount(availableQtyUnformatted)
          : availableQtyUnformatted.toLocaleString();

      let badgeClass = "bg-info";
      let warningText = "";

      if (availableQtyUnformatted < 0) {
        badgeClass = "bg-danger";
        warningText = " (Negative stock!)";
        showNotification(
          `⚠️ Warning: Stock for this item is negative (${availableQty}). Please review inventory adjustments.`,
          "error",
        );
      } else if (availableQty === 0) {
        badgeClass = "bg-warning";
        warningText = " (Out of stock)";
      }

      // Build the display HTML
      let displayHtml = `<div class="badge ${badgeClass}">
    <span>Available: ${availableQty}${warningText}</span>`;

      // Add average cost only for purchase forms
      if (isPurchaseForm && avgCost > 0) {
        const currencyCode = baseCurrency.code;
        displayHtml += ` <span class="ms-2">| Avg Cost: ${formatCurrency(avgCost, currencyCode, true)}</span>`;
      }

      displayHtml += `</div>`;

      if (stockDisplay) {
        stockDisplay.innerHTML = displayHtml;
      }
    })
    .catch((error) => {
      console.error("Error fetching available quantity:", error);
      if (stockDisplay) {
        stockDisplay.innerHTML =
          '<div class="badge bg-danger">Error loading quantity</div>';
      }
    });
}

// Initialize item type fields on page load
document.addEventListener("DOMContentLoaded", function () {
  document.querySelectorAll(".item-type-field").forEach((field) => {
    const lineItem = field.closest(".line-item");
    if (field.value === "inventory") {
      showInventoryFields(lineItem);
    } else {
      showRegularFields(lineItem);
    }
  });

  // ✅ Format all existing line item totals on page load
  document.querySelectorAll(".line-item").forEach((lineItem) => {
    updateLineItemTotal(lineItem);
  });
});

// Add this after your DOM is ready
document.addEventListener("change", function (event) {
  // Handle item type changes
  if (event.target.classList.contains("item-type-field")) {
    const lineItem = event.target.closest(".line-item");
    if (event.target.value === "inventory") {
      showInventoryFields(lineItem);
    } else {
      showRegularFields(lineItem);
    }
    // Update totals and summary after toggling fields
    updateLineItemTotal(lineItem);
    updateSummary();
  }

  // Handle inventory dropdown changes
  if (event.target.classList.contains("inventory-dropdown")) {
    const lineItem = event.target.closest(".line-item");
    const itemTypeField = lineItem.querySelector(".item-type-field");
    if (itemTypeField.value === "inventory") {
      updateUOMForInventoryItem(lineItem);
    }
  }
});

// Add this to your existing event listeners
document.addEventListener("change", function (event) {
  if (event.target.classList.contains("inventory-dropdown")) {
    const lineItem = event.target.closest(".line-item");
    updateUOMForInventoryItem(lineItem);
  }
});

// Function to add discount field
function addDiscountField(lineItem, addButton, container) {
  // Prevent adding multiple discounts by checking if one already exists
  if (container.querySelector(".discount-field")) {
    return;
  }

  const discountField = document.createElement("div");
  discountField.classList.add("form-group", "discount-field");
  discountField.innerHTML = `
        <label>Discount</label>

           <div class="form-grid">
        <select name="discount_type[]">
            <option value="amount">Amount</option>
            <option value="percentage">Percentage</option>
        </select>
        <input type="text" class="line-discount" name="discount_value[]" placeholder="Value">

        </div>
        <button type="button" class="remove-discount remove-button">Remove</button>
    `;

  container.appendChild(discountField);

  // Hide the "Add Discount" button after adding a discount
  addButton.style.display = "none";

  // Add event listeners for the new discount field
  const discountTypeSelect = discountField.querySelector(
    'select[name="discount_type[]"]',
  );
  const discountValueInput = discountField.querySelector(
    'input[name="discount_value[]"]',
  );
  const removeButton = discountField.querySelector(".remove-discount");

  discountTypeSelect.addEventListener("change", function () {
    updateLineItemTotal(lineItem);
    updateSummary();
  });

  discountValueInput.addEventListener("input", function () {
    updateLineItemTotal(lineItem);
    updateSummary();
  });

  removeButton.addEventListener("click", function () {
    discountField.remove();
    addButton.style.display = "inline-block";
    updateLineItemTotal(lineItem);
    updateSummary();
  });
}

// Function to add tax field
function addTaxField(lineItem, addButton, container) {
  // Prevent adding multiple tax fields by checking if one already exists
  if (container.querySelector(".tax-field")) {
    return;
  }

  const taxField = document.createElement("div");
  taxField.classList.add("form-group", "tax-field");
  taxField.innerHTML = `
        <label>Tax (%)</label>
          <div class="form-grid">
        <input type="text" name="tax[]" placeholder="Tax (%)" min="0">
        </div>
        <button type="button" class="remove-tax remove-button">Remove</button>
    `;

  container.appendChild(taxField);

  // Hide the "Add Tax" button after adding a tax field
  addButton.style.display = "none";

  // Add event listeners for the new tax field
  const taxInput = taxField.querySelector('input[name="tax[]"]');
  const removeButton = taxField.querySelector(".remove-tax");

  taxInput.addEventListener("input", function () {
    updateLineItemTotal(lineItem);
    updateSummary();
  });

  removeButton.addEventListener("click", function () {
    taxField.remove();
    addButton.style.display = "inline-block";
    updateLineItemTotal(lineItem);
    updateSummary();
  });
}

// Function to add all necessary event listeners to a line item
function addLineItemEventListeners(lineItem) {
  // Quantity and price listeners
  const quantityField = lineItem.querySelector('input[name="quantity[]"]');
  const unitPriceField = lineItem.querySelector('input[name="unit_price[]"]');

  quantityField.addEventListener("input", function () {
    updateLineItemTotal(lineItem);
    updateSummary();
  });

  unitPriceField.addEventListener("input", function () {
    updateLineItemTotal(lineItem);
    updateSummary();
  });

  // Item type change listener
  const itemTypeField = lineItem.querySelector(".item-type-field");
  itemTypeField.addEventListener("change", function () {
    if (this.value === "inventory") {
      showInventoryFields(lineItem);
      // If switching to inventory and item is already selected, update UOM
      const inventoryDropdown = lineItem.querySelector(".inventory-dropdown");
      if (inventoryDropdown.value) {
        updateUOMForInventoryItem(lineItem);
      }
    } else {
      showRegularFields(lineItem);
    }
    updateLineItemTotal(lineItem);
    updateSummary();
  });

  // Inventory dropdown change listener
  const inventoryDropdown = lineItem.querySelector(".inventory-dropdown");
  inventoryDropdown.addEventListener("change", function () {
    if (itemTypeField.value === "inventory") {
      updateUOMForInventoryItem(lineItem);
      updateAvailableQuantity(lineItem);
    }
  });

  // Warehouse dropdown change listener
  const warehouseDropdown = lineItem.querySelector(".warehouse-dropdown");
  if (warehouseDropdown) {
    warehouseDropdown.addEventListener("change", function () {
      updateAvailableQuantity(lineItem);
    });
  }

  // UOM dropdown change listener (for non-inventory items)
  const uomDropdown = lineItem.querySelector(".uom-dropdown");
  uomDropdown.addEventListener("change", function () {
    const uomHidden = lineItem.querySelector(".uom-hidden");
    uomHidden.value = this.value;
  });

  // Remove line item listener
  // const removeButton = lineItem.querySelector(".remove-line-item");
  // if (removeButton) {
  //   removeButton.addEventListener("click", function () {
  //     lineItem.remove();
  //     updateSummary();
  //   });
  // }

  // Add discount functionality
  const addDiscountButton = lineItem.querySelector(".add-discount");
  const discountTaxFields = lineItem.querySelector(".discount-tax-fields");

  if (addDiscountButton) {
    addDiscountButton.addEventListener("click", function () {
      addDiscountField(lineItem, addDiscountButton, discountTaxFields);
    });
  }

  // Add tax functionality
  const addTaxButton = lineItem.querySelector(".add-tax");

  if (addTaxButton) {
    addTaxButton.addEventListener("click", function () {
      addTaxField(lineItem, addTaxButton, discountTaxFields);
    });
  }

  // Also add listeners for existing discount/tax fields if any
  const existingDiscountFields = lineItem.querySelectorAll(".discount-field");
  existingDiscountFields.forEach((field) => {
    field
      .querySelector('select[name="discount_type[]"]')
      ?.addEventListener("change", function () {
        updateLineItemTotal(lineItem);
        updateSummary();
      });
    field
      .querySelector('input[name="discount_value[]"]')
      ?.addEventListener("input", function () {
        updateLineItemTotal(lineItem);
        updateSummary();
      });
  });

  const existingTaxFields = lineItem.querySelectorAll(".tax-field");
  existingTaxFields.forEach((field) => {
    field
      .querySelector('input[name="tax[]"]')
      ?.addEventListener("input", function () {
        updateLineItemTotal(lineItem);
        updateSummary();
      });
  });
}

// Function to initialize UOM for existing line items on page load
// Function to initialize UOM for existing line items on page load
function initializeExistingLineItems() {
  console.log("Initializing existing line items (template-rendered)...");

  document.querySelectorAll(".line-item").forEach((lineItem, index) => {
    console.log(`Processing template-rendered line item ${index + 1}`);

    const itemTypeField = lineItem.querySelector(".item-type-field");
    const inventoryDropdown = lineItem.querySelector(".inventory-dropdown");

    // For template-rendered items, we need to handle Select2 differently
    // because they haven't been initialized yet

    if (itemTypeField.value === "inventory") {
      console.log("Initializing inventory item from template");

      // First hide/show the right fields WITHOUT destroying Select2
      // (since it's not initialized yet)
      const itemField = lineItem.querySelector(".item-field");
      const warehouseGroup = lineItem.querySelector(".warehouse-group");
      const uomReadonly = lineItem.querySelector(".uom-readonly");
      const uomDropdown = lineItem.querySelector(".uom-dropdown");
      const itemDescriptionGroup = lineItem.querySelector(
        ".item-description-group",
      );
      const itemDescriptionField = lineItem.querySelector(
        ".item-description-field",
      );

      // Manual field show/hide for template items
      if (itemField) itemField.style.display = "none";
      if (inventoryDropdown) inventoryDropdown.style.display = "block";
      if (warehouseGroup) warehouseGroup.style.display = "block";
      if (uomReadonly) uomReadonly.style.display = "block";
      if (uomDropdown) uomDropdown.style.display = "none";

      // HIDE item description for inventory items
      if (itemDescriptionGroup) itemDescriptionGroup.style.display = "none";
      if (itemDescriptionField) {
        itemDescriptionField.disabled = true;
        itemDescriptionField.classList.add("disabled-field");
      }

      // Now initialize Select2
      if (inventoryDropdown) {
        $(inventoryDropdown)
          .select2({
            width: "100%",
            placeholder: "Select inventory item",
          })
          .on("change", function () {
            updateAvailableQuantity(lineItem);
            updateUOMForInventoryItem(lineItem);
          });
      }

      const warehouseDropdown = lineItem.querySelector(".warehouse-dropdown");
      if (warehouseDropdown) {
        $(warehouseDropdown)
          .select2({
            width: "100%",
            placeholder: "Select warehouse",
          })
          .on("change", function () {
            updateAvailableQuantity(lineItem);
          });
      }

      if (inventoryDropdown && inventoryDropdown.value) {
        updateUOMForInventoryItem(lineItem);
      }
    } else {
      console.log("Initializing non-inventory item from template");

      // For non-inventory items, just set display properties
      // Don't call showRegularFields because it tries to destroy Select2
      const itemField = lineItem.querySelector(".item-field");
      const inventoryDropdown = lineItem.querySelector(".inventory-dropdown");
      const warehouseGroup = lineItem.querySelector(".warehouse-group");
      const uomReadonly = lineItem.querySelector(".uom-readonly");
      const uomDropdown = lineItem.querySelector(".uom-dropdown");
      const itemDescriptionGroup = lineItem.querySelector(
        ".item-description-group",
      );
      const itemDescriptionField = lineItem.querySelector(
        ".item-description-field",
      );

      // Manual field show/hide
      if (itemField) {
        itemField.style.display = "block";
        itemField.disabled = false;
        itemField.classList.remove("disabled-field");
      }
      if (inventoryDropdown) inventoryDropdown.style.display = "none";
      if (warehouseGroup) warehouseGroup.style.display = "none";
      if (uomReadonly) uomReadonly.style.display = "none";
      if (uomDropdown) {
        uomDropdown.style.display = "block";
        uomDropdown.setAttribute("required", "required");
      }

      // SHOW item description for non-inventory items
      if (itemDescriptionGroup) itemDescriptionGroup.style.display = "block";
      if (itemDescriptionField) {
        itemDescriptionField.disabled = false;
        itemDescriptionField.classList.remove("disabled-field");
      }
    }

    // Add event listeners (but skip the ones that would destroy Select2)
    addBasicEventListeners(lineItem);
  });

  console.log("Template-rendered line items initialized");
}
// Simpler event listener function for template items
function addBasicEventListeners(lineItem) {
  // Quantity and price listeners
  const quantityField = lineItem.querySelector('input[name="quantity[]"]');
  const unitPriceField = lineItem.querySelector('input[name="unit_price[]"]');

  if (quantityField) {
    quantityField.addEventListener("input", function () {
      updateLineItemTotal(lineItem);
      updateSummary();
    });
  }

  if (unitPriceField) {
    unitPriceField.addEventListener("input", function () {
      updateLineItemTotal(lineItem);
      updateSummary();
    });
  }

  // Item type change listener
  const itemTypeField = lineItem.querySelector(".item-type-field");
  if (itemTypeField) {
    itemTypeField.addEventListener("change", function () {
      if (this.value === "inventory") {
        showInventoryFields(lineItem);
        const inventoryDropdown = lineItem.querySelector(".inventory-dropdown");
        if (inventoryDropdown && inventoryDropdown.value) {
          updateUOMForInventoryItem(lineItem);
        }
      } else {
        showRegularFields(lineItem);
      }
      updateLineItemTotal(lineItem);
      updateSummary();
    });
  }
}

// Function to add event listeners to quantity and unit price fields
function addQuantityAndPriceListeners(lineItem) {
  const quantityField = lineItem.querySelector('input[name="quantity[]"]');
  const unitPriceField = lineItem.querySelector('input[name="unit_price[]"]');

  // Add event listener to quantity field
  if (quantityField) {
    quantityField.addEventListener("input", () => {
      updateLineItemTotal(lineItem);
      updateSummary();
    });
  }

  // Add event listener to unit price field
  if (unitPriceField) {
    unitPriceField.addEventListener("input", () => {
      updateLineItemTotal(lineItem);
      updateSummary();
    });
  }
}

// Function to add event listeners to discount and tax fields
function addDiscountAndTaxListeners(lineItem) {
  const discountTypeField = lineItem.querySelector(
    'select[name="discount_type[]"]',
  );
  const discountValueField = lineItem.querySelector(
    'input[name="discount_value[]"]',
  );
  const taxField = lineItem.querySelector('input[name="tax[]"]');

  // Add event listeners to discount type and discount value fields
  if (discountTypeField) {
    discountTypeField.addEventListener("change", () => {
      updateLineItemTotal(lineItem);
      updateSummary();
    });
  }

  if (discountValueField) {
    discountValueField.addEventListener("input", () => {
      updateLineItemTotal(lineItem);
      updateSummary();
    });
  }

  // Add event listener to tax field
  if (taxField) {
    taxField.addEventListener("input", () => {
      updateLineItemTotal(lineItem);
      updateSummary();
    });
  }
}

function setupLineItemDeletion() {
  if (window.lineItemDeletionInitialized) {
    return;
  }
  window.lineItemDeletionInitialized = true;

  let currentLineItemToDelete = null;
  const modal = document.getElementById("confirmationModal");
  const confirmBtn = document.getElementById("confirmDelete");
  const cancelBtn = document.getElementById("cancelDelete");

  // Event delegation for all line item interactions
  document
    .getElementById("lineItemsContainer")
    .addEventListener("click", function (e) {
      // Handle remove buttons
      if (e.target.classList.contains("remove-line-item")) {
        e.preventDefault();
        e.stopPropagation();
        currentLineItemToDelete = e.target.closest(".line-item");
        modal.style.display = "block";
      }

      // Handle discount/tax remove buttons
      if (
        e.target.classList.contains("remove-discount") ||
        e.target.classList.contains("remove-tax")
      ) {
        e.target.closest(".discount-field, .tax-field").remove();
        const lineItem = e.target.closest(".line-item");
        updateLineItemTotal(lineItem);
        updateSummary();
      }
    });

  // Also handle input changes through delegation
  document
    .getElementById("lineItemsContainer")
    .addEventListener("input", function (e) {
      const lineItem = e.target.closest(".line-item");
      if (lineItem) {
        if (
          e.target.name === "quantity[]" ||
          e.target.name === "unit_price[]" ||
          e.target.name === "discount_value[]" ||
          e.target.name === "tax[]"
        ) {
          updateLineItemTotal(lineItem);
          updateSummary();
        }
      }
    });

  confirmBtn.onclick = function () {
    if (currentLineItemToDelete) {
      currentLineItemToDelete.remove();

      // Renumber remaining line items
      const lineItems = document.querySelectorAll(".line-item");
      lineItems.forEach((item, idx) => {
        const header = item.querySelector(".line-item-header h3");
        if (header) {
          header.textContent = `Line Item ${idx + 1}`;
        }
      });

      updateSummary();
      currentLineItemToDelete = null;
    }
    modal.style.display = "none";
  };

  cancelBtn.onclick = function () {
    modal.style.display = "none";
    currentLineItemToDelete = null;
  };

  modal.addEventListener("click", function (e) {
    if (e.target === this) {
      this.style.display = "none";
      currentLineItemToDelete = null;
    }
  });
}

// Function to show or hide the tax allocation section based on total tax
function toggleTaxAllocationSection() {
  const totalTax =
    parseFloat(document.getElementById("totalTax").textContent) || 0;
  const taxPayableAccountSection = document.getElementById(
    "taxAllocationSection",
  );
  const taxAccountSelect = document.getElementById("taxAccount");

  console.log("=== TAX SECTION DEBUG ===");
  console.log("Total Tax:", totalTax);
  console.log("Section visible:", totalTax > 0);
  console.log(
    "Tax Account required attribute:",
    taxAccountSelect.hasAttribute("required"),
  );
  console.log("Tax Account value:", taxAccountSelect.value);

  if (taxPayableAccountSection) {
    if (totalTax > 0) {
      taxPayableAccountSection.style.display = "block";
      taxAccountSelect.setAttribute("required", "required");
      console.log("Section shown, required added");
    } else {
      taxPayableAccountSection.style.display = "none";
      taxAccountSelect.removeAttribute("required");
      taxAccountSelect.value = "";
      console.log("Section hidden, required removed");
    }
  }
  console.log("=== END DEBUG ===");
}

function updateRequiredAttributes(row) {
  let itemTypeSelect = row.querySelector(".item-type-field");
  let itemField = row.querySelector(".item-field");
  let inventoryDropdown = row.querySelector(".inventory-dropdown");
  let warehouseDropdown = row.querySelector(".warehouse-dropdown");

  if (itemTypeSelect.value === "inventory") {
    console.log(
      "Inventory item selected. Setting inventory dropdown as required.",
    );
    inventoryDropdown.setAttribute("required", "required");
    warehouseDropdown.setAttribute("required", "required");
    itemField.removeAttribute("required");
  } else {
    console.log(
      `Non-inventory item selected (${itemTypeSelect.value}). Setting item field as required.`,
    );
    itemField.setAttribute("required", "required");
    inventoryDropdown.removeAttribute("required");
    warehouseDropdown.removeAttribute("required");
  }
}

// Function to parse formatted currency values
function parseFormattedCurrency(value) {
  if (typeof value !== "string") value = String(value);
  // Remove commas and any other non-numeric characters except decimal point
  const numericString = value.replace(/[^\d.]/g, "");
  return parseFloat(numericString) || 0;
}

// SIMPLE VALIDATION FUNCTION - Just call this before form submission
function validateInventoryItems() {
  const lineItems = document.querySelectorAll(".line-item");
  const issues = [];

  lineItems.forEach((row, index) => {
    const itemType = row.querySelector(".item-type-field")?.value;

    // Only check inventory items
    if (itemType === "inventory") {
      const itemDropdown = row.querySelector(".inventory-dropdown");
      const selectedOption = itemDropdown?.options[itemDropdown.selectedIndex];

      if (selectedOption && selectedOption.value) {
        // Check if this item has the warning class/marker
        const hasStockIssue =
          selectedOption.text.includes("(No Stock)") ||
          row.querySelector(".stock-warning");

        if (hasStockIssue) {
          issues.push(`Line ${index + 1}: "${selectedOption.text.trim()}"`);
        }
      }
    }
  });

  if (issues.length > 0) {
    const message =
      "The following inventory items have no purchase records:\n\n" +
      issues.join("\n") +
      "\n\nYou can still sell them, but you won't be able to delete or update this transaction later. Continue?";

    return confirm(message);
  }

  return true; // No issues found
}

// Optional: For existing line items that have no warehouse selected
function setDefaultWarehouseForEmptyLineItems() {
  const globalWarehouse = document.getElementById("location");
  if (!globalWarehouse || !globalWarehouse.value) return;

  document.querySelectorAll(".line-item").forEach((lineItem) => {
    const itemType = lineItem.querySelector(".item-type-field")?.value;
    if (itemType === "inventory") {
      const warehouseDropdown = lineItem.querySelector(".warehouse-dropdown");
      // Only set if no warehouse is currently selected
      if (warehouseDropdown && !$(warehouseDropdown).val()) {
        $(warehouseDropdown).val(globalWarehouse.value).trigger("change");
      }
    }
  });
}
