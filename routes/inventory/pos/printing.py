


# Add these imports at the top
import os
from datetime import datetime
from escpos.printer import Dummy
import subprocess

# Add this route to your Flask app
@app.route('/inventory/pos/api/print-receipt', methods=['POST'])
def print_receipt():
    try:
        data = request.get_json()

        # Create receipt content
        receipt_text = generate_receipt_text(data)

        # Try to print (will fallback to different methods)
        success = attempt_printing(receipt_text)

        if success:
            return jsonify({'success': True, 'message': 'Receipt printed successfully'})
        else:
            # Save as fallback
            filename = save_receipt_as_text(receipt_text, data['receipt_number'])
            return jsonify({
                'success': False,
                'message': 'Printed to file',
                'filename': filename
            })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

def generate_receipt_text(data):
    """Generate formatted receipt text"""
    dummy = Dummy()

    # Header
    dummy.set(align='center', width=2, height=2)
    dummy.text("MY STORE\n")
    dummy.set(align='center')
    dummy.text("123 Main Street\n")
    dummy.text("(555) 123-4567\n")
    dummy.text("=" * 32 + "\n")

    # Transaction info
    dummy.set(align='left')
    dummy.text(f"Date: {data['date']}\n")
    dummy.text(f"Receipt: #{data['receipt_number']}\n")
    dummy.text(f"Customer: {data['customer'][:20]}\n")
    dummy.text("-" * 32 + "\n")

    # Items
    dummy.text("QTY  DESCRIPTION       PRICE\n")
    dummy.text("-" * 32 + "\n")

    for item in data['items']:
        name = item['name'][:16]  # Truncate long names
        qty = str(item['quantity']).ljust(3)
        price = f"{item['price']:.2f}"
        total = f"{item['price'] * item['quantity']:.2f}"

        dummy.text(f"{qty} {name.ljust(16)} {price.rjust(6)}\n")

    # Totals
    dummy.text("-" * 32 + "\n")
    dummy.text(f"Subtotal: {data['subtotal']:>8.2f}\n")
    if data['discount'] > 0:
        dummy.text(f"Discount: -{data['discount']:>7.2f}\n")
    dummy.text(f"Tax: {data['tax']:>8.2f}\n")
    dummy.set(bold=True)
    dummy.text(f"TOTAL: {data['total']:>8.2f}\n")
    dummy.set(bold=False)

    # Payment info
    dummy.text("-" * 32 + "\n")
    dummy.text(f"Payment: {data['payment_method']}\n")
    dummy.text(f"Processed by: {data['processed_by']}\n")
    dummy.text("=" * 32 + "\n")
    dummy.set(align='center')
    dummy.text("Thank you for your business!\n")
    dummy.text("\n\n\n")  # Feed paper

    return dummy.output

def attempt_printing(receipt_text):
    """Try different printing methods"""
    try:
        # Method 1: Network printer (most common)
        try:
            printer = Network("192.168.1.100", port=9100, timeout=3)
            printer._raw(receipt_text)
            printer.cut()
            return True
        except:
            pass

        # Method 2: USB printer
        try:
            # Common USB printer IDs - adjust for your printer
            printer = Usb(0x04b8, 0x0202)  # Epson TM-T88V
            printer._raw(receipt_text)
            printer.cut()
            return True
        except:
            pass

        # Method 3: Windows printer sharing
        try:
            if os.name == 'nt':  # Windows
                with open('temp_receipt.txt', 'w', encoding='utf-8') as f:
                    f.write(receipt_text.decode('utf-8'))
                subprocess.run(['notepad', '/p', 'temp_receipt.txt'], check=True)
                os.remove('temp_receipt.txt')
                return True
        except:
            pass

        return False

    except Exception:
        return False

def save_receipt_as_text(receipt_text, receipt_number):
    """Save receipt as text file fallback"""
    filename = f"receipt_{receipt_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    filepath = os.path.join('receipts', filename)

    os.makedirs('receipts', exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(receipt_text.decode('utf-8'))

    return filename
