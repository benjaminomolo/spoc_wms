/**
 * Exchange Rate Module - Reusable functions for multi-currency support
 *
 * Dependencies:
 * - jQuery (optional, for better AJAX)
 * - csrfToken must be available globally
 */

const ExchangeRateManager = {
  /**
   * Initialize exchange rate functionality for a form
   * @param {Object} config - Configuration object
   * @param {string} config.currencySelectId - ID of currency select element
   * @param {string} config.dateInputId - ID of date input element
   * @param {string} config.exchangeRateInputId - ID of exchange rate input
   * @param {string} config.containerId - ID of exchange rate container
   * @param {string} config.fromDisplayId - ID of from currency display span
   * @param {string} config.toDisplayId - ID of to currency display span
   * @param {string} config.baseCurrencyId - ID of hidden base currency input
   * @param {number|string} config.appId - Application/Company ID
   * @param {string} config.apiEndpoint - API endpoint for fetching rates
   * @param {string} config.csrfToken - CSRF token
   * @param {Object} config.callbacks - Optional callbacks
   */
  init(config) {
    // Store configuration
    this.config = {
      currencySelectId: "form_currency",
      dateInputId: "date",
      exchangeRateInputId: "exchange_rate",
      containerId: "exchange_rate_container",
      fromDisplayId: "from_currency_display",
      toDisplayId: "to_currency_display",
      baseCurrencyId: "base_currency_id",
      apiEndpoint: "/multi_currency/api/get_exchange_rate",
      ...config,
    };

    // Get DOM elements
    this.currencySelect = document.getElementById(this.config.currencySelectId);
    this.dateInput = document.getElementById(this.config.dateInputId);
    this.exchangeRateInput = document.getElementById(
      this.config.exchangeRateInputId,
    );
    this.container = document.getElementById(this.config.containerId);
    this.fromDisplay = document.getElementById(this.config.fromDisplayId);
    this.toDisplay = document.getElementById(this.config.toDisplayId);
    this.baseCurrencyInput = document.getElementById(
      this.config.baseCurrencyId,
    );

    // Validate required elements
    this._validateElements();

    // Store initial values if in edit mode
    this.initialCurrencyId = this.currencySelect?.value;
    this.initialExchangeRate = this.exchangeRateInput?.value;

    // Set up event listeners
    this._setupEventListeners();

    // Initialize visibility
    this.toggleVisibility();

    console.log("ExchangeRateManager initialized", {
      currency: this.initialCurrencyId,
      baseCurrency: window.baseCurrency,
    });

    return this;
  },

  /**
   * Validate that all required DOM elements exist
   */
  _validateElements() {
    const required = [
      { el: this.currencySelect, name: "currencySelect" },
      { el: this.dateInput, name: "dateInput" },
      { el: this.exchangeRateInput, name: "exchangeRateInput" },
      { el: this.container, name: "container" },
      { el: this.fromDisplay, name: "fromDisplay" },
      { el: this.toDisplay, name: "toDisplay" },
      { el: this.baseCurrencyInput, name: "baseCurrencyInput" },
    ];

    const missing = required
      .filter((item) => !item.el)
      .map((item) => item.name);

    if (missing.length > 0) {
      console.warn("ExchangeRateManager: Missing DOM elements:", missing);
    }
  },

  /**
   * Set up event listeners
   */
  _setupEventListeners() {
    // Currency change - handle both native and Select2
    if (this.currencySelect) {
      // For native selects
      this.currencySelect.addEventListener("change", () =>
        this.toggleVisibility(),
      );

      // For Select2 (if jQuery is available)
      if (typeof $ !== "undefined") {
        $(this.currencySelect).on("change.select2", () => {
          this.toggleVisibility();
        });
      }
    }

    // Date change
    if (this.dateInput) {
      this.dateInput.addEventListener("change", () => {
        if (this.container?.style.display === "block") {
          this.fetchRate();
        }
      });
    }

    // Manual rate change (for recalculating totals)
    if (this.exchangeRateInput) {
      this.exchangeRateInput.addEventListener("input", () => {
        if (typeof this.config.callbacks?.onRateChange === "function") {
          this.config.callbacks.onRateChange(this.exchangeRateInput.value);
        }
      });
    }
  },

  /**
   * Toggle exchange rate field visibility based on currency selection
   */
  toggleVisibility() {
    const selectedCurrencyId = this.currencySelect?.value;

    if (
      selectedCurrencyId &&
      window.baseCurrency &&
      selectedCurrencyId !== window.baseCurrency.id
    ) {
      // Show the container
      this._showExchangeRate();

      // Fetch rate if currency changed or no rate exists
      if (
        selectedCurrencyId !== this.initialCurrencyId ||
        !this.initialExchangeRate
      ) {
        this.fetchRate();
      }
    } else {
      // Hide the container
      this._hideExchangeRate();
    }
  },

  /**
   * Show exchange rate field
   */
  _showExchangeRate() {
    if (!this.currencySelect || !this.container) return;

    const selectedOption =
      this.currencySelect.options[this.currencySelect.selectedIndex];
    const selectedCurrencyCode = selectedOption?.text || "";

    this.fromDisplay.textContent = `1 ${selectedCurrencyCode}`;
    this.toDisplay.textContent = window.baseCurrency?.code || "";
    this.container.style.display = "block";

    if (this.exchangeRateInput) {
      this.exchangeRateInput.required = true;
    }

    // Call the visibility change callback
    if (typeof this.config.callbacks?.onVisibilityChange === "function") {
      this.config.callbacks.onVisibilityChange(true);
    }
  },

  /**
   * Hide exchange rate field
   */

  _hideExchangeRate() {
    if (this.container) {
      this.container.style.display = "none";
    }
    if (this.exchangeRateInput) {
      this.exchangeRateInput.required = false;
      this.exchangeRateInput.value = "";
    }

    // Call the visibility change callback
    if (typeof this.config.callbacks?.onVisibilityChange === "function") {
      this.config.callbacks.onVisibilityChange(false);
    }
  },

  /**
   * Fetch exchange rate from API
   */
  fetchRate() {
    const selectedCurrencyId = this.currencySelect?.value;
    const selectedDate = this.dateInput?.value;

    if (
      !selectedCurrencyId ||
      !window.baseCurrency ||
      selectedCurrencyId === window.baseCurrency.id ||
      !selectedDate
    ) {
      return;
    }

    // Show loading state
    if (this.exchangeRateInput) {
      this.exchangeRateInput.placeholder = "Loading...";
      this.exchangeRateInput.disabled = true;
    }

    // Call before fetch callback
    if (typeof this.config.callbacks?.beforeFetch === "function") {
      this.config.callbacks.beforeFetch();
    }

    // Prepare request data
    const requestData = {
      from_currency_id: selectedCurrencyId,
      to_currency_id: window.baseCurrency.id,
      date: selectedDate,
      app_id: this.config.appId,
    };

    // Make the API call
    fetch(this.config.apiEndpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": this.config.csrfToken,
      },
      body: JSON.stringify(requestData),
    })
      .then((response) => response.json())
      .then((data) => {
        if (data.success) {
          // Update rate if conditions met
          if (
            selectedCurrencyId !== this.initialCurrencyId ||
            !this.initialExchangeRate
          ) {
            if (this.exchangeRateInput) {
              this.exchangeRateInput.value = data.rate;
            }
          }

          // Reset input state
          if (this.exchangeRateInput) {
            this.exchangeRateInput.placeholder = "Enter rate";
            this.exchangeRateInput.disabled = false;
          }

          console.log(`Rate loaded from ${data.source} data`);

          // Call success callback
          if (typeof this.config.callbacks?.onSuccess === "function") {
            this.config.callbacks.onSuccess(data.rate, data.source);
          }
        } else {
          // No rate found
          if (
            selectedCurrencyId !== this.initialCurrencyId ||
            !this.initialExchangeRate
          ) {
            if (this.exchangeRateInput) {
              this.exchangeRateInput.value = "";
            }
          }

          if (this.exchangeRateInput) {
            this.exchangeRateInput.placeholder = "Enter rate (manual)";
            this.exchangeRateInput.disabled = false;
          }

          // Call error callback
          if (typeof this.config.callbacks?.onError === "function") {
            this.config.callbacks.onError("No rate found");
          }
        }
      })
      .catch((error) => {
        console.error("Error fetching rate:", error);

        if (this.exchangeRateInput) {
          this.exchangeRateInput.value = "";
          this.exchangeRateInput.placeholder = "Enter rate (manual)";
          this.exchangeRateInput.disabled = false;
        }

        // Call error callback
        if (typeof this.config.callbacks?.onError === "function") {
          this.config.callbacks.onError(error);
        }
      });
  },

  /**
   * Reset to initial state
   */
  reset(currencyId = null, rate = null) {
    // Reset currency if provided
    if (currencyId && this.currencySelect) {
      for (let i = 0; i < this.currencySelect.options.length; i++) {
        if (this.currencySelect.options[i].value == currencyId) {
          this.currencySelect.selectedIndex = i;
          break;
        }
      }
    }

    // Reset rate
    if (this.exchangeRateInput) {
      this.exchangeRateInput.value = rate || "";
    }

    // Toggle visibility
    this.toggleVisibility();
  },

  /**
   * Get current exchange rate value
   */
  getRate() {
    return this.exchangeRateInput
      ? parseFloat(this.exchangeRateInput.value) || 0
      : 0;
  },

  /**
   * Check if foreign currency is selected
   */
  isForeignCurrency() {
    const selectedId = this.currencySelect?.value;
    return !!(
      selectedId &&
      window.baseCurrency &&
      selectedId !== window.baseCurrency.id
    );
  },

  /**
   * Get base currency code
   */
  getBaseCurrencyCode() {
    return window.baseCurrency?.code || "";
  },
};

// Make it available globally
window.ExchangeRateManager = ExchangeRateManager;

function fetchMiniExchangeRates() {
  if (!window.baseCurrency) {
    console.log("baseCurrency not available yet");
    return;
  }

  const endDate = new Date().toISOString().split("T")[0];
  const baseCurrency = window.baseCurrency.code;
  const baseCurrencyId = window.baseCurrency.id;

  console.log("Fetching rates with:", {
    endDate,
    baseCurrency,
    baseCurrencyId,
  });

  $.ajax({
    url: `/multi_currency/api/exchange-rates?date=${endDate}&base_currency=${baseCurrency}&base_currency_id=${baseCurrencyId}`,
    type: "GET",
    success: function (response) {
      console.log("Rates response:", response);
      if (
        response.success &&
        response.rates &&
        Object.keys(response.rates).length > 0
      ) {
        let ratesHtml =
          '<div style="display: flex; flex-wrap: wrap; gap: 4px;">';
        const currencies = Object.keys(response.rates).sort();

        currencies.forEach((currency) => {
          const rate = response.rates[currency];
          ratesHtml += `
                        <span style="background: #f8f9fa; padding: 2px 6px; border-radius: 10px; border: 1px solid #e9ecef; font-size: 0.7rem;">
                            <strong>1 ${currency}</strong> ${formatNumber(rate, 2)}
                        </span>
                    `;
        });

        ratesHtml += "</div>";
        $("#fx-rates-mini-list").html(ratesHtml);

        if (response.date) {
          $("#fx-rates-date").text(`as at ${formatDate(response.date)}`);
        }
      } else {
        $("#fx-rates-mini-list").html(
          '<small style="font-size: 0.7rem; color: #adb5bd;">No rates available</small>',
        );
      }
    },
    error: function (xhr, status, error) {
      console.error("Rates error:", { xhr, status, error });
      $("#fx-rates-mini-list").html(
        '<small style="font-size: 0.7rem; color: #dc3545;">Failed to load</small>',
      );
    },
  });
}
