-- Integration test database initialization script

-- Create test tables
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    stock INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    total DECIMAL(10, 2) NOT NULL,
    status ENUM('pending', 'completed', 'cancelled') DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB;

-- Insert test data
INSERT INTO users (username, email, created_at) VALUES
    ('alice', 'alice@example.com', '2024-01-01 10:00:00'),
    ('bob', 'bob@example.com', '2024-01-02 11:00:00'),
    ('charlie', 'charlie@example.com', '2024-01-03 12:00:00'),
    ('david', 'david@example.com', '2024-01-04 13:00:00'),
    ('eve', 'eve@example.com', '2024-01-05 14:00:00');

INSERT INTO products (name, price, stock) VALUES
    ('Widget A', 19.99, 100),
    ('Widget B', 29.99, 50),
    ('Gadget X', 49.99, 75),
    ('Gadget Y', 99.99, 25),
    ('Tool Z', 149.99, 10);

INSERT INTO orders (user_id, total, status, created_at) VALUES
    (1, 59.97, 'completed', '2024-01-10 10:00:00'),
    (2, 149.99, 'completed', '2024-01-11 11:00:00'),
    (3, 99.99, 'pending', '2024-01-12 12:00:00'),
    (1, 29.99, 'completed', '2024-01-13 13:00:00'),
    (4, 199.98, 'cancelled', '2024-01-14 14:00:00');

-- Create a table for testing exclusion patterns
CREATE TABLE IF NOT EXISTS users_backup (
    id INT PRIMARY KEY,
    data TEXT
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS tmp_cache (
    id INT PRIMARY KEY,
    value VARCHAR(255)
) ENGINE=InnoDB;
