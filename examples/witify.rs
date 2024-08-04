use std::path::Path;

use anyhow::Result;
use semver::Version;
use sqwit::Generator;

fn main() -> Result<()> {
    let mut generator = Generator::new("example-namespace", "example", Some(Version::new(0, 1, 0)));

    // Users table
    let users_sql = r#"
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE,
        age INTEGER,
        is_active BOOLEAN DEFAULT true,
        balance NUMERIC(10,2),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    )
    "#;
    generator.add_table(users_sql)?;

    // Products table
    let products_sql = r#"
    CREATE TABLE products (
        id BIGSERIAL PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        description TEXT,
        price DECIMAL(10,2) NOT NULL,
        stock_quantity INTEGER NOT NULL DEFAULT 0,
        category VARCHAR(50),
        is_available BOOLEAN DEFAULT true,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        last_updated TIMESTAMP WITH TIME ZONE
    )
    "#;
    generator.add_table(products_sql)?;

    // Orders table
    let orders_sql = r#"
    CREATE TABLE orders (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id INTEGER REFERENCES users(id) NOT NULL,
        order_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
        total_amount DECIMAL(12,2) NOT NULL,
        status VARCHAR(20) CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
        shipping_address TEXT NOT NULL,
        billing_address TEXT NULL
    )
    "#;
    generator.add_table(orders_sql)?;

    // Reviews table
    let reviews_sql = r#"
    CREATE TABLE reviews (
        id SERIAL PRIMARY KEY,
        product_id BIGINT REFERENCES products(id),
        user_id INTEGER REFERENCES users(id),
        rating SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
        comment TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        helpful_votes INTEGER DEFAULT 0,
        is_verified BOOLEAN DEFAULT false
    )
    "#;
    generator.add_table(reviews_sql)?;

    // Specify the output directory
    let output_dir = Path::new("./output");

    // Write the WIT types to a file
    generator.write_to_file(output_dir)?;

    println!("WIT types have been generated and written to file.");

    Ok(())
}
