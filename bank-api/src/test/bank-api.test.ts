import { describe, test, expect, beforeAll, afterAll } from 'vitest';
import { BankAPI, emptyBankState, ACCOUNT_STATE, utils, type BankProviders } from '../index.js';
import pino from 'pino';
import { firstValueFrom, filter } from 'rxjs';
import WebSocket from 'ws';
import { TestEnvironment, TestProviders } from './commons.js';
import path from 'node:path';
import fs from 'node:fs';
import { currentDir } from './config.js';

describe('BankAPI', () => {
  test('should have correct initial empty state', () => {
    expect(emptyBankState.accountExists).toBe(false);
    expect(emptyBankState.balance).toBe(null); // Encrypted balance system - null until authenticated
    expect(emptyBankState.transactionCount).toBe(0n);
    expect(emptyBankState.whoami).toBe('unknown');
    expect(emptyBankState.accountStatus).toBe(ACCOUNT_STATE.inactive);
    expect(emptyBankState.transactionHistory).toEqual([]);
  });

  // Minimal smoke: basic exports
  test('should export core types', () => {
    expect(BankAPI).toBeDefined();
    expect(emptyBankState).toBeDefined();
    expect(ACCOUNT_STATE).toBeDefined();
    expect(utils).toBeDefined();
  });

  describe('Utils', () => {
    test('should format balance correctly', () => {
      expect(utils.formatBalance(10000n)).toBe('100.00');
      expect(utils.formatBalance(2550n)).toBe('25.50');
      expect(utils.formatBalance(0n)).toBe('0.00');
    });

    test('should parse amounts correctly', () => {
      expect(utils.parseAmount('100.00')).toBe(10000n);
      expect(utils.parseAmount('25.50')).toBe(2550n);
      expect(utils.parseAmount('0.01')).toBe(1n);
    });

    test('should handle pad function', () => {
      const result = utils.pad('test', 10);
      expect(result.length).toBe(10);
      expect(result[0]).toBe(116); // 't' in ASCII
    });

    test('should generate random bytes', () => {
      const bytes1 = utils.randomBytes(32);
      const bytes2 = utils.randomBytes(32);
      
      expect(bytes1.length).toBe(32);
      expect(bytes2.length).toBe(32);
      expect(bytes1).not.toEqual(bytes2); // Should be different
    });
  });

  describe('ACCOUNT_STATE enum', () => {
    test('should have correct values', () => {
      expect(ACCOUNT_STATE.inactive).toBe(0);
      expect(ACCOUNT_STATE.active).toBe(1);
      expect(ACCOUNT_STATE.verified).toBe(2);
      expect(ACCOUNT_STATE.suspended).toBe(3);
    });
  });

  // Drop signature/unit tests; integration covers behavior

  describe('Integration', () => {
    let testEnvironment: TestEnvironment;
    let providers: BankProviders;
    const logFile = path.resolve(currentDir, '..', 'logs', 'tests', `${new Date().toISOString()}.log`);
    fs.mkdirSync(path.dirname(logFile), { recursive: true });
    const logger = pino(
      { level: process.env.LOG_LEVEL ?? 'info' },
      // Write synchronously to a file so logs always persist
      pino.destination({ dest: logFile, sync: true }),
    );

    beforeAll(async () => {
      // Ensure WebSocket global is set for indexer WS
      globalThis.WebSocket = WebSocket as unknown as typeof globalThis.WebSocket;
      testEnvironment = new TestEnvironment(logger);
      const testConfiguration = await testEnvironment.start();
      const wallet = await testEnvironment.getWallet1();
      providers = await new TestProviders().configureBankProviders(wallet, testConfiguration.dappConfig);
    }, 10 * 60_000);

    afterAll(async () => {
      await testEnvironment.shutdown();
    });


    test('should run full lifecycle: create, auth balance, deposit, withdraw, verify', async () => {
      const userId = `lifecycle-user-${Date.now()}`;

      logger.info('Deploying Bank contract for lifecycle test…');
      const contractAddress = await BankAPI.deploy(providers, logger);

      // Create account with $50.00
      await BankAPI.createAccount(providers, contractAddress, userId, '1234', '50.00', logger);
      const bankAPI = await BankAPI.subscribe(userId, providers, contractAddress, logger);
      const afterCreate = await firstValueFrom(
        bankAPI.state$.pipe(filter((s) => s.accountExists === true && s.balance === 5000n)),
      );
      expect(afterCreate.accountExists).toBe(true);
      expect(afterCreate.balance).toBe(5000n);

      // Authenticate balance access (no state changes expected)
      await bankAPI.getTokenBalance('1234');

      // Deposit $25.00 -> 7500
      await bankAPI.deposit('1234', '25.00');
      const afterDeposit = await firstValueFrom(
        bankAPI.state$.pipe(filter((s) => s.balance === 7500n)),
      );
      expect(afterDeposit.balance).toBe(7500n);

      // Withdraw $10.00 -> 6500
      await bankAPI.withdraw('1234', '10.00');
      const afterWithdraw = await firstValueFrom(
        bankAPI.state$.pipe(filter((s) => s.balance === 6500n)),
      );
      expect(afterWithdraw.balance).toBe(6500n);

      // Verify account status -> should become verified
      await bankAPI.verifyAccountStatus('1234');
      const afterVerify = await firstValueFrom(
        bankAPI.state$.pipe(filter((s) => s.accountStatus === ACCOUNT_STATE.verified)),
      );
      expect(afterVerify.accountStatus).toBe(ACCOUNT_STATE.verified);

      logger.info('Fetching detailed transaction history…');
      // Assert detailed client-side log order and fields
      const detailed = await bankAPI.getDetailedTransactionHistory();
      expect(Array.isArray(detailed)).toBe(true);
      expect(detailed.length).toBeGreaterThanOrEqual(5);
      const last5 = detailed.slice(-5);
      expect(last5.map((d) => d.type)).toEqual(['create', 'auth', 'deposit', 'withdraw', 'verify']);

      logger.info('Checking detailed transaction history…');
      // Amount presence
      expect(last5[0].amount).toBeDefined(); // create
      expect(last5[1].amount).toBeUndefined(); // auth
      expect(last5[2].amount).toBeDefined(); // deposit
      expect(last5[3].amount).toBeDefined(); // withdraw
      expect(last5[4].amount).toBeUndefined(); // verify
      // Amount values (in cents)
      expect(last5[0].amount).toBe(5000n); // create $50.00
      expect(last5[2].amount).toBe(2500n); // deposit $25.00
      expect(last5[3].amount).toBe(1000n); // withdraw $10.00
      // balanceAfter recorded as bigint
      expect(typeof last5[0].balanceAfter).toBe('bigint');

      // no-op
    }, 10 * 60_000);


    test('should handle transfer errors: insufficient funds, wrong PIN, and account creation validation', async () => {
      const aliceUserId = `alice-errors-${Date.now()}`;
      const bobUserId = `bob-errors-${Date.now()}`;
      const charlieUserId = `charlie-errors-${Date.now()}`;

      logger.info('Testing transfer errors and account creation validation…');
      const contractAddress = await BankAPI.deploy(providers, logger);

      // Test account creation validation - Alice with $20, Bob with $50
      await BankAPI.createAccount(providers, contractAddress, aliceUserId, '1111', '20.00', logger);
      await BankAPI.createAccount(providers, contractAddress, bobUserId, '2222', '50.00', logger);
      const aliceBankAPI = await BankAPI.subscribe(aliceUserId, providers, contractAddress, logger);
      const bobBankAPI = await BankAPI.subscribe(bobUserId, providers, contractAddress, logger);
      
      // Verify accounts were created with correct balances
      await firstValueFrom(aliceBankAPI.state$.pipe(filter((s) => s.accountExists === true && s.balance === 2000n)));
      await firstValueFrom(bobBankAPI.state$.pipe(filter((s) => s.accountExists === true && s.balance === 5000n)));
      
      // Test 1: Insufficient funds - Alice tries to transfer $50 (more than her $20 balance)
      await expect(async () => {
        await aliceBankAPI.transferToUser('1111', bobUserId, '50.00');
      }).rejects.toThrow(); // Should fail with "Insufficient funds for transfer"
      
      // Test 2: Wrong PIN - Alice tries transfer with wrong PIN
      await expect(async () => {
        await aliceBankAPI.transferToUser('9999', bobUserId, '10.00'); // Wrong PIN
      }).rejects.toThrow(); // Should fail authentication
      
      // Test 3: Account creation validation - create Charlie with proper amount
      await BankAPI.createAccount(providers, contractAddress, charlieUserId, '3333', '100.00', logger);
      const charlieBankAPI = await BankAPI.subscribe(charlieUserId, providers, contractAddress, logger);
      const charlieReady = await firstValueFrom(
        charlieBankAPI.state$.pipe(filter((s) => s.accountExists === true && s.balance === 10000n)),
      );
      
      // Verify Charlie's account was created successfully with correct properties
      expect(charlieReady.accountExists).toBe(true);
      expect(charlieReady.balance).toBe(10000n);
      expect(charlieReady.accountStatus === ACCOUNT_STATE.active || charlieReady.accountStatus === ACCOUNT_STATE.verified).toBe(true);
      
      logger.info('Transfer errors and account creation tests completed');
    }, 10 * 60_000);


    // Zelle-like Authorization System Tests
    describe('Authorization System', () => {
      test('should complete full authorization workflow with encrypted token claim', async () => {
        const aliceUserId = `alice-auth-${Date.now()}`;
        const bobUserId = `bob-auth-${Date.now()}`;

        logger.info('Testing full authorization workflow with claim…');
        const contractAddress = await BankAPI.deploy(providers, logger);

        // Setup accounts: Alice($200), Bob($100)
        await BankAPI.createAccount(providers, contractAddress, aliceUserId, '1111', '200.00', logger);
        await BankAPI.createAccount(providers, contractAddress, bobUserId, '2222', '100.00', logger);
        const aliceBankAPI = await BankAPI.subscribe(aliceUserId, providers, contractAddress, logger);
        const bobBankAPI = await BankAPI.subscribe(bobUserId, providers, contractAddress, logger);
        
        await firstValueFrom(aliceBankAPI.state$.pipe(filter((s) => s.balance === 20000n)));
        await firstValueFrom(bobBankAPI.state$.pipe(filter((s) => s.balance === 10000n)));

        // Step 1: Alice requests authorization to send to Bob
        await aliceBankAPI.requestTransferAuthorization('1111', bobUserId);

        // Step 2: Bob approves Alice's request with $50 limit
        await bobBankAPI.approveTransferAuthorization('2222', aliceUserId, '50.00');

        // Step 3: Alice sends $30 to Bob (encrypted on blockchain)
        await aliceBankAPI.sendToAuthorizedUser('1111', bobUserId, '30.00');
        
        // Wait for Alice's balance to update (200 - 30 = 170)
        await firstValueFrom(aliceBankAPI.state$.pipe(filter((s) => s.balance === 17000n)));
        
        // Give Bob's API time to see the blockchain state update
        await new Promise(resolve => setTimeout(resolve, 2000));
        
        // Verify Bob's balance hasn't changed yet (pending claim)
        const bobBeforeClaim = await firstValueFrom(bobBankAPI.state$.pipe(filter((s) => s.balance === 10000n)));
        expect(bobBeforeClaim.balance).toBe(10000n);

        // Step 4: Bob detects and claims the encrypted transfer
        const pendingClaims = await bobBankAPI.getPendingClaims();
        logger.info(`Pending claims found: ${pendingClaims.length}`, { claims: pendingClaims });
        expect(pendingClaims).toHaveLength(1);
        expect(pendingClaims[0].senderUserId).toBe(aliceUserId);

        await bobBankAPI.claimAuthorizedTransfer('2222', aliceUserId);
        
        // Verify Bob received exact amount (100 + 30 = 130)
        const bobAfterClaim = await firstValueFrom(bobBankAPI.state$.pipe(filter((s) => s.balance === 13000n)));
        expect(bobAfterClaim.balance).toBe(13000n);

        // Verify transaction histories include claim
        const aliceHistory = await aliceBankAPI.getDetailedTransactionHistory();
        const bobHistory = await bobBankAPI.getDetailedTransactionHistory();
        
        // Alice should have auth_request and auth_transfer records
        const aliceAuthRequest = aliceHistory.find(tx => tx.type === 'auth_request');
        const aliceAuthTransfer = aliceHistory.find(tx => tx.type === 'auth_transfer');
        
        expect(aliceAuthRequest).toBeDefined();
        expect(aliceAuthTransfer?.amount).toBe(3000n); // $30.00 in cents

        // Bob should have auth_approve and claim_transfer records
        const bobAuthApproval = bobHistory.find(tx => tx.type === 'auth_approve');
        const bobClaim = bobHistory.find(tx => tx.type === 'claim_transfer');
        
        expect(bobAuthApproval).toBeDefined();
        expect(bobAuthApproval?.maxAmount).toBe(5000n); // $50.00 in cents
        expect(bobClaim).toBeDefined();
        expect(bobClaim?.amount).toBe(3000n); // Exact $30.00 recovered

        logger.info('Full authorization workflow with claim completed successfully');
      }, 15 * 60_000);

      test('should allow multiple transfers with encrypted tokens and claims', async () => {
        const aliceUserId = `alice-multi-auth-${Date.now()}`;
        const bobUserId = `bob-multi-auth-${Date.now()}`;

        const contractAddress = await BankAPI.deploy(providers, logger);

        // Setup accounts
        await BankAPI.createAccount(providers, contractAddress, aliceUserId, '1111', '150.00', logger);
        await BankAPI.createAccount(providers, contractAddress, bobUserId, '2222', '50.00', logger);
        const aliceBankAPI = await BankAPI.subscribe(aliceUserId, providers, contractAddress, logger);
        const bobBankAPI = await BankAPI.subscribe(bobUserId, providers, contractAddress, logger);
        
        await firstValueFrom(aliceBankAPI.state$.pipe(filter((s) => s.balance === 15000n)));
        await firstValueFrom(bobBankAPI.state$.pipe(filter((s) => s.balance === 5000n)));

        // Setup authorization: Bob allows Alice to send up to $80
        await aliceBankAPI.requestTransferAuthorization('1111', bobUserId);
        await bobBankAPI.approveTransferAuthorization('2222', aliceUserId, '80.00');

        // Alice makes multiple encrypted transfers
        await aliceBankAPI.sendToAuthorizedUser('1111', bobUserId, '20.00');
        await firstValueFrom(aliceBankAPI.state$.pipe(filter((s) => s.balance === 13000n))); // 150-20=130

        await aliceBankAPI.sendToAuthorizedUser('1111', bobUserId, '25.00');
        await firstValueFrom(aliceBankAPI.state$.pipe(filter((s) => s.balance === 10500n))); // 130-25=105

        await aliceBankAPI.sendToAuthorizedUser('1111', bobUserId, '15.00');
        await firstValueFrom(aliceBankAPI.state$.pipe(filter((s) => s.balance === 9000n))); // 105-15=90

        // Bob's balance should still be original (pending claims)
        const bobBeforeClaims = await firstValueFrom(bobBankAPI.state$.pipe(filter((s) => s.balance === 5000n)));
        expect(bobBeforeClaims.balance).toBe(5000n);

        // Bob should see pending claims but amounts are encrypted (0n until claimed)
        await new Promise(resolve => setTimeout(resolve, 2000));
        const pendingClaims = await bobBankAPI.getPendingClaims();
        expect(pendingClaims).toHaveLength(1); // Multiple transfers to same auth get combined

        // Bob claims the combined encrypted transfer
        await bobBankAPI.claimAuthorizedTransfer('2222', aliceUserId);
        
        // Bob should receive total of all transfers: $50 + $60 = $110
        await firstValueFrom(bobBankAPI.state$.pipe(filter((s) => s.balance === 11000n))); // 50+60=110

        // Verify transaction histories
        const aliceHistory = await aliceBankAPI.getDetailedTransactionHistory();
        const bobHistory = await bobBankAPI.getDetailedTransactionHistory();
        
        const aliceAuthTransfers = aliceHistory.filter(tx => tx.type === 'auth_transfer');
        const bobClaims = bobHistory.filter(tx => tx.type === 'claim_transfer');
        
        expect(aliceAuthTransfers).toHaveLength(3); // 3 individual sends
        expect(bobClaims).toHaveLength(1); // 1 combined claim
        expect(bobClaims[0].amount).toBe(6000n); // $60 total claimed

        logger.info('Multiple encrypted transfers and claim test completed');
      }, 15 * 60_000);

      test('should handle authorization errors: exceed limit, no auth, no pending request, and legacy transfer', async () => {
        const aliceUserId = `alice-auth-errors-${Date.now()}`;
        const bobUserId = `bob-auth-errors-${Date.now()}`;
        const charlieUserId = `charlie-auth-errors-${Date.now()}`;
        const davidUserId = `david-auth-errors-${Date.now()}`;

        const contractAddress = await BankAPI.deploy(providers, logger);

        // Setup accounts for all error scenarios
        await BankAPI.createAccount(providers, contractAddress, aliceUserId, '1111', '200.00', logger);
        await BankAPI.createAccount(providers, contractAddress, bobUserId, '2222', '100.00', logger);
        await BankAPI.createAccount(providers, contractAddress, charlieUserId, '3333', '100.00', logger);
        await BankAPI.createAccount(providers, contractAddress, davidUserId, '4444', '50.00', logger);
        
        const aliceBankAPI = await BankAPI.subscribe(aliceUserId, providers, contractAddress, logger);
        const bobBankAPI = await BankAPI.subscribe(bobUserId, providers, contractAddress, logger);
        const charlieBankAPI = await BankAPI.subscribe(charlieUserId, providers, contractAddress, logger);
        const davidBankAPI = await BankAPI.subscribe(davidUserId, providers, contractAddress, logger);
        
        await firstValueFrom(aliceBankAPI.state$.pipe(filter((s) => s.balance === 20000n)));
        await firstValueFrom(bobBankAPI.state$.pipe(filter((s) => s.balance === 10000n)));
        await firstValueFrom(charlieBankAPI.state$.pipe(filter((s) => s.balance === 10000n)));
        await firstValueFrom(davidBankAPI.state$.pipe(filter((s) => s.balance === 5000n)));

        // Test 1: Authorization limit exceeded
        await aliceBankAPI.requestTransferAuthorization('1111', bobUserId);
        await bobBankAPI.approveTransferAuthorization('2222', aliceUserId, '30.00'); // $30 limit
        await expect(async () => {
          await aliceBankAPI.sendToAuthorizedUser('1111', bobUserId, '50.00'); // Exceeds $30 limit
        }).rejects.toThrow(); // Should fail "Amount exceeds authorized limit"
        
        // Test 2: Transfer without authorization
        await expect(async () => {
          await charlieBankAPI.sendToAuthorizedUser('3333', davidUserId, '25.00'); // No auth exists
        }).rejects.toThrow(); // Should fail "No authorization - recipient must approve first"
        
        // Test 3: Approval without pending request
        await expect(async () => {
          await davidBankAPI.approveTransferAuthorization('4444', charlieUserId, '50.00'); // No request
        }).rejects.toThrow(); // Should fail "No pending authorization request"
        
        // Test 4: Legacy transferToUser without authorization (helpful error message)
        await expect(async () => {
          await charlieBankAPI.transferToUser('3333', davidUserId, '25.00'); // Legacy method
        }).rejects.toThrow(/Transfer failed: No authorization exists.*authorization system/);
        
        logger.info('Authorization error scenarios test completed');
      }, 10 * 60_000);

      test('should handle bidirectional authorization, double-claim prevention, and encrypted claims', async () => {
        const aliceUserId = `alice-advanced-${Date.now()}`;
        const bobUserId = `bob-advanced-${Date.now()}`;

        const contractAddress = await BankAPI.deploy(providers, logger);

        // Setup accounts
        await BankAPI.createAccount(providers, contractAddress, aliceUserId, '1111', '150.00', logger);
        await BankAPI.createAccount(providers, contractAddress, bobUserId, '2222', '100.00', logger);
        const aliceBankAPI = await BankAPI.subscribe(aliceUserId, providers, contractAddress, logger);
        const bobBankAPI = await BankAPI.subscribe(bobUserId, providers, contractAddress, logger);

        // Setup bidirectional authorizations
        await aliceBankAPI.requestTransferAuthorization('1111', bobUserId);
        await bobBankAPI.approveTransferAuthorization('2222', aliceUserId, '100.00'); // Higher limit for double-claim test
        
        await bobBankAPI.requestTransferAuthorization('2222', aliceUserId);
        await aliceBankAPI.approveTransferAuthorization('1111', bobUserId, '60.00');

        // Both users send encrypted transfers
        await aliceBankAPI.sendToAuthorizedUser('1111', bobUserId, '50.00');   // Alice -> Bob (for double-claim test)
        await firstValueFrom(aliceBankAPI.state$.pipe(filter((s) => s.balance === 10000n))); // 150-50=100

        await bobBankAPI.sendToAuthorizedUser('2222', aliceUserId, '25.00');   // Bob -> Alice  
        await firstValueFrom(bobBankAPI.state$.pipe(filter((s) => s.balance === 7500n))); // 100-25=75

        // Balances haven't changed yet for recipients (pending claims)
        const aliceBeforeClaim = await firstValueFrom(aliceBankAPI.state$.pipe(filter((s) => s.balance === 10000n)));
        const bobBeforeClaim = await firstValueFrom(bobBankAPI.state$.pipe(filter((s) => s.balance === 7500n)));
        expect(aliceBeforeClaim.balance).toBe(10000n); // Still just sent amount deducted
        expect(bobBeforeClaim.balance).toBe(7500n);    // Still just sent amount deducted

        // Bob claims Alice's transfer
        await new Promise(resolve => setTimeout(resolve, 2000));
        await bobBankAPI.claimAuthorizedTransfer('2222', aliceUserId);    // Bob claims Alice's $50
        await firstValueFrom(bobBankAPI.state$.pipe(filter((s) => s.balance === 12500n))); // 75+50=125
        
        // Test double-claim prevention - Bob tries to claim the same transfer again
        await expect(async () => {
          await bobBankAPI.claimAuthorizedTransfer('2222', aliceUserId);
        }).rejects.toThrow(); // Should fail "No pending amount to claim"
        
        // Bob's balance should remain unchanged after failed double-claim
        const bobAfterDoubleClaim = await firstValueFrom(bobBankAPI.state$.pipe(filter((s) => s.balance === 12500n)));
        expect(bobAfterDoubleClaim.balance).toBe(12500n);
        
        // Alice claims Bob's transfer
        await aliceBankAPI.claimAuthorizedTransfer('1111', bobUserId);    // Alice claims Bob's $25
        await firstValueFrom(aliceBankAPI.state$.pipe(filter((s) => s.balance === 12500n))); // 100+25=125

        // Verify transaction histories include both sends and claims
        const aliceHistory = await aliceBankAPI.getDetailedTransactionHistory();
        const bobHistory = await bobBankAPI.getDetailedTransactionHistory();
        
        const aliceAuthTransfers = aliceHistory.filter(tx => tx.type === 'auth_transfer');
        const aliceClaims = aliceHistory.filter(tx => tx.type === 'claim_transfer');
        const bobAuthTransfers = bobHistory.filter(tx => tx.type === 'auth_transfer');
        const bobClaims = bobHistory.filter(tx => tx.type === 'claim_transfer');
        
        expect(aliceAuthTransfers).toHaveLength(1); // Alice sent $50
        expect(aliceClaims).toHaveLength(1);        // Alice claimed $25
        expect(bobAuthTransfers).toHaveLength(1);   // Bob sent $25
        expect(bobClaims).toHaveLength(1);          // Bob claimed $50

        expect(aliceClaims[0].amount).toBe(2500n);  // $25 claimed
        expect(bobClaims[0].amount).toBe(5000n);    // $50 claimed

        logger.info('Bidirectional authorization and double-claim prevention test completed');
      }, 15 * 60_000);
    });

    describe('Selective Balance Disclosure API', () => {
      test('should complete full disclosure workflow: grant, verify, list, revoke', async () => {
        const aliceUserId = 'alice-disclosure';
        const bobUserId = 'bob-disclosure';
        const charlieUserId = 'charlie-disclosure';

        logger.info('Testing selective balance disclosure API…');
        const contractAddress = await BankAPI.deploy(providers, logger);

        // Create accounts: Alice ($200), Bob ($150), Charlie ($100)
        await BankAPI.createAccount(providers, contractAddress, aliceUserId, '1111', '200.00', logger);
        await BankAPI.createAccount(providers, contractAddress, bobUserId, '2222', '150.00', logger);
        await BankAPI.createAccount(providers, contractAddress, charlieUserId, '3333', '100.00', logger);
        
        const aliceBankAPI = await BankAPI.subscribe(aliceUserId, providers, contractAddress, logger);
        const bobBankAPI = await BankAPI.subscribe(bobUserId, providers, contractAddress, logger);
        const charlieBankAPI = await BankAPI.subscribe(charlieUserId, providers, contractAddress, logger);

        await firstValueFrom(aliceBankAPI.state$.pipe(filter((s) => s.balance === 20000n)));
        await firstValueFrom(bobBankAPI.state$.pipe(filter((s) => s.balance === 15000n)));
        await firstValueFrom(charlieBankAPI.state$.pipe(filter((s) => s.balance === 10000n)));

        // Test 1: Alice grants threshold permission to Bob (never expires)
        await aliceBankAPI.grantDisclosurePermission('1111', bobUserId, 'threshold', '100.00', 0);
        
        // Test 2: Bob verifies Alice has >= $100 (should be true, Alice has $200)
        const hasThreshold = await bobBankAPI.verifyBalanceThreshold('2222', aliceUserId, '100.00');
        expect(hasThreshold).toBe(true);

        // Test 3: Bob tries to verify higher threshold than authorized (should fail)
        await expect(async () => {
          await bobBankAPI.verifyBalanceThreshold('2222', aliceUserId, '150.00');
        }).rejects.toThrow(/Threshold amount.*exceeds authorized maximum/); // Should fail threshold validation

        // Test 4: Alice grants exact disclosure to Charlie with expiration
        const expiresAt = new Date(Date.now() + 2 * 60 * 60 * 1000); // 2 hours from now
        await aliceBankAPI.grantDisclosurePermissionUntil('1111', charlieUserId, 'exact', '0.00', expiresAt);

        // Test 5: Charlie gets Alice's exact balance
        const aliceBalance = await charlieBankAPI.getDisclosedBalance('3333', aliceUserId);
        expect(aliceBalance).toBe(20000n); // Alice has $200

        // Test 6: Alice lists her granted permissions
        const permissions = await aliceBankAPI.getDisclosurePermissions('1111');
        expect(permissions).toHaveLength(2);
        
        const bobPermission = permissions.find(p => p.requesterId === bobUserId);
        const charliePermission = permissions.find(p => p.requesterId === charlieUserId);
        
        expect(bobPermission).toBeDefined();
        expect(bobPermission?.permissionType).toBe('threshold');
        expect(bobPermission?.thresholdAmount).toBe(10000n);
        expect(bobPermission?.expiresAt).toBe(null); // Never expires
        
        expect(charliePermission).toBeDefined();
        expect(charliePermission?.permissionType).toBe('exact');
        expect(charliePermission?.expiresAt).toBeInstanceOf(Date);

        // Test 7: Alice revokes Bob's permission
        await aliceBankAPI.revokeDisclosurePermission('1111', bobUserId);

        // Test 8: Bob should no longer be able to verify threshold (after a short wait for revocation to take effect)
        await new Promise(resolve => setTimeout(resolve, 3000)); // Wait 3 seconds for revocation
        await expect(async () => {
          await bobBankAPI.verifyBalanceThreshold('2222', aliceUserId, '100.00');
        }).rejects.toThrow(); // Should fail "Disclosure permission has expired"

        // Test 9: Charlie should still have access (not revoked)
        const aliceBalanceAgain = await charlieBankAPI.getDisclosedBalance('3333', aliceUserId);
        expect(aliceBalanceAgain).toBe(20000n);

        logger.info('Selective balance disclosure API test completed successfully');
      }, 15 * 60_000);

      test('should handle disclosure errors: no permission, wrong PIN, self-disclosure', async () => {
        const aliceUserId = 'alice-errors';
        const bobUserId = 'bob-errors';

        logger.info('Testing disclosure error scenarios…');
        const contractAddress = await BankAPI.deploy(providers, logger);

        await BankAPI.createAccount(providers, contractAddress, aliceUserId, '1111', '100.00', logger);
        await BankAPI.createAccount(providers, contractAddress, bobUserId, '2222', '50.00', logger);
        
        const aliceBankAPI = await BankAPI.subscribe(aliceUserId, providers, contractAddress, logger);
        const bobBankAPI = await BankAPI.subscribe(bobUserId, providers, contractAddress, logger);

        await firstValueFrom(aliceBankAPI.state$.pipe(filter((s) => s.balance === 10000n)));
        await firstValueFrom(bobBankAPI.state$.pipe(filter((s) => s.balance === 5000n)));

        // Test 1: Bob tries to verify Alice's balance without permission
        await expect(async () => {
          await bobBankAPI.verifyBalanceThreshold('2222', aliceUserId, '50.00');
        }).rejects.toThrow(/Target user has no recipient authorizations|No disclosure permission found/); // Should throw error when no permission exists

        // Test 2: Bob tries to get Alice's exact balance without permission
        await expect(async () => {
          await bobBankAPI.getDisclosedBalance('2222', aliceUserId);
        }).rejects.toThrow(/Target user has no recipient authorizations|No exact disclosure permission found/); // Should fail with ledger lookup error

        // Test 3: Alice tries to grant permission with wrong PIN
        await expect(async () => {
          await aliceBankAPI.grantDisclosurePermission('9999', bobUserId, 'threshold', '50.00', 24);
        }).rejects.toThrow(); // Should fail authentication

        // Test 4: Alice tries to grant self-disclosure
        await expect(async () => {
          await aliceBankAPI.grantDisclosurePermission('1111', aliceUserId, 'exact', '0.00', 24);
        }).rejects.toThrow(); // Should fail "Cannot grant disclosure to yourself"

        // Test 5: Invalid expiration date (past date)
        const pastDate = new Date(Date.now() - 60 * 60 * 1000); // 1 hour ago
        await expect(async () => {
          await aliceBankAPI.grantDisclosurePermissionUntil('1111', bobUserId, 'threshold', '50.00', pastDate);
        }).rejects.toThrow('Expiration date must be in the future');

        logger.info('Disclosure error scenarios test completed');
      }, 10 * 60_000);

      test('should handle absolute date conversion correctly', async () => {
        const aliceUserId = 'alice-dates';
        const bobUserId = 'bob-dates';

        logger.info('Testing absolute date conversion…');
        const contractAddress = await BankAPI.deploy(providers, logger);

        await BankAPI.createAccount(providers, contractAddress, aliceUserId, '1111', '100.00', logger);
        await BankAPI.createAccount(providers, contractAddress, bobUserId, '2222', '50.00', logger);
        
        const aliceBankAPI = await BankAPI.subscribe(aliceUserId, providers, contractAddress, logger);
        const bobBankAPI = await BankAPI.subscribe(bobUserId, providers, contractAddress, logger);

        await firstValueFrom(aliceBankAPI.state$.pipe(filter((s) => s.balance === 10000n)));

        // STEP 1: Sync contract time with real time so grant_disclosure_permission uses correct current_timestamp
        // Contract starts at epoch 1,000,000 seconds. Advance it to align with current real time.
        const currentRealTimeSeconds = Math.floor(Date.now() / 1000);
        logger.info(`Syncing contract time: advancing by ${currentRealTimeSeconds} seconds to match real time`);
        await aliceBankAPI.setContractTime(currentRealTimeSeconds);

        // STEP 2: Now grant permission - the contract will use its current_timestamp (now synced) as reference
        const exactlyOneDayFromNow = new Date(Date.now() + 24 * 60 * 60 * 1000);
        
        logger.info(`Granting permission that expires at: ${exactlyOneDayFromNow.toISOString()}`);
        
        // Grant permission using absolute date - contract calculates hours from its current_timestamp to this target
        await aliceBankAPI.grantDisclosurePermissionUntil('1111', bobUserId, 'threshold', '50.00', exactlyOneDayFromNow);

        // Bob should be able to verify immediately (permission is active)
        const hasThreshold = await bobBankAPI.verifyBalanceThreshold('2222', aliceUserId, '50.00');
        expect(hasThreshold).toBe(true);

        // STEP 3: Check that the permission was stored with exact expiration
        const permissions = await aliceBankAPI.getDisclosurePermissions('1111');
        expect(permissions).toHaveLength(1);
        
        const permission = permissions[0];
        expect(permission.requesterId).toBe(bobUserId);
        expect(permission.expiresAt).toBeInstanceOf(Date);
        
        // Since we synced contract time perfectly, the timestamps should match exactly
        logger.info(`Stored expiration: ${permission.expiresAt?.toISOString()}, Expected: ${exactlyOneDayFromNow.toISOString()}`);
        
        expect(permission.expiresAt).not.toBe(null);
        // Contract stores timestamps in seconds, compare in seconds not milliseconds
        const expectedSeconds = Math.floor(exactlyOneDayFromNow.getTime() / 1000);
        const actualSeconds = Math.floor(permission.expiresAt!.getTime() / 1000);
        
        // Allow small tolerance for blockchain execution time (within 60 seconds)
        const timeDifferenceSeconds = Math.abs(actualSeconds - expectedSeconds);
        expect(timeDifferenceSeconds).toBeLessThan(60);

        // STEP 4: Test expiration by advancing contract time past the expiration
        logger.info('Testing expiration: advancing contract time by 25 hours (past 24-hour expiration)');
        await aliceBankAPI.setContractTime(currentRealTimeSeconds + 25 * 3600); // 25 hours forward (1 hour past expiration)

        // Bob should no longer be able to verify (permission expired)
        await expect(async () => {
          await bobBankAPI.verifyBalanceThreshold('2222', aliceUserId, '50.00');
        }).rejects.toThrow(/Disclosure permission has expired/); // Should throw error because permission expired

        logger.info('Contract time sync and expiration test completed successfully');
      }, 10 * 60_000);
    });
  });
});