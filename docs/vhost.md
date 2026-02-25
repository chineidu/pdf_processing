# RabbitMQ Virtual Host Setup Guide (For Development)

This guide provides step-by-step instructions for creating and configuring RabbitMQ virtual hosts for the Named Entity Extraction project.

## What is a Virtual Host?

A virtual host in RabbitMQ is a way to segregate applications using the same RabbitMQ instance. It allows you to have multiple isolated environments within a single RabbitMQ server, each with its own:

- Exchanges
- Queues
- Bindings
- User permissions
- Policies

Virtual hosts are identified by names starting with a forward slash (e.g., `/decide`, `/production`, `/staging`).

## Prerequisites

- Docker and Docker Compose installed
- RabbitMQ services running (see main README for startup instructions)
- Access to RabbitMQ management interface (default: <http://localhost:15672>)

## Option 1: Automated Setup (Recommended)

For convenience, you can use the provided initialization script that automatically creates the virtual host using environment variables from your `.env` file.

### 1. Make the Script Executable (One-time setup)

```bash
chmod +x docker/init-rabbitmq-vhost.sh
```

**Note:** You only need to run this command once after creating or downloading the script. The execute permission persists until the file is recreated.

### 2. Run the Initialization Script

After starting your RabbitMQ services with `docker-compose up -d`, run the initialization script:

```bash
./docker/init-rabbitmq-vhost.sh
```

The script will:

- Load configuration from your `.env` file
- Wait for RabbitMQ to be ready
- Create the `/decide` virtual host
- Set permissions for the configured user
- Verify the setup

## One-liner Setup

```bash
docker-compose up -d && sleep 8 && chmod +x docker/init-rabbitmq-vhost.sh && docker/init-rabbitmq-vhost.sh
```

## Option 2: Create Virtual Host via Management UI

### 1. Access RabbitMQ Management Interface

1. Open <http://localhost:15672> in your browser
2. Login with default credentials (username: `your_username`, password: `your_password`)

### 2. Create the Virtual Host

1. Navigate to the "Admin" tab in the top navigation
2. Click on "Virtual Hosts" in the left sidebar
3. Click the "Add a new virtual host" button
4. In the "Name" field, enter `/decide` i.e., the name of your virtual host
5. Click "Add virtual host"

### 3. Set User Permissions

1. With the `/decide` virtual host selected, click on "Set permission"
2. In the user field, enter `guest` (or your desired username)
3. Set the following permissions:
   - **Configure regexp**: `.*`
   - **Write regexp**: `.*`
   - **Read regexp**: `.*`
4. Click "Set permission"

## Verification (Both Options)

### 4. Verify Virtual Host Creation

You can verify the virtual host was created successfully by:

#### Via Command Line

```bash
docker exec -it local-rabbitmq rabbitmqctl list_vhosts
```

#### Via Management UI

1. Open <http://localhost:15672> in your browser
2. Login with default credentials (guest/guest)
3. Navigate to the "Admin" tab
4. Click on "Virtual Hosts"
5. You should see `/decide` in the list

### Useful Commands

- **List all virtual hosts:**

  ```bash
  docker exec -it local-rabbitmq rabbitmqctl list_vhosts
  ```

- **List permissions for a virtual host:**

  ```bash
  docker exec -it local-rabbitmq rabbitmqctl list_permissions -p /decide
  ```

- **Delete a virtual host (if needed):**

  ```bash
  docker exec -it local-rabbitmq rabbitmqctl delete_vhost /decide
  ```

## Best Practices

1. **Naming Convention**: Use descriptive names starting with `/` (e.g., `/production`, `/staging`, `/development`)

2. **User Management**: Create specific users for each virtual host rather than using the default `guest` user

3. **Permissions**: Follow the principle of least privilege - grant only necessary permissions

4. **Monitoring**: Regularly monitor virtual host usage through the management interface

5. **Backup**: Include virtual host configurations in your backup strategy

## Additional Resources

- [RabbitMQ Virtual Hosts Documentation](https://www.rabbitmq.com/vhosts.html)
- [RabbitMQ Management Interface](https://www.rabbitmq.com/management.html)
- [RabbitMQ Access Control](https://www.rabbitmq.com/access-control.html)
